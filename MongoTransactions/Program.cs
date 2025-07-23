using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Transactions;

namespace MongoTransactions;

public class User
{
    [BsonId]
    public ObjectId Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

public class Order
{
    [BsonId]
    public ObjectId Id { get; set; }
    public ObjectId UserId { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

class Program
{
    private static readonly string ConnectionString = "mongodb://localhost:27027,localhost:27028,localhost:27029/?replicaSet=rs1&readPreference=secondary";
    private static readonly string DatabaseName = "TransactionDemo";

    static async Task Main(string[] args)
    {
        Console.WriteLine("MongoDB Transactional Insert Demo");
        Console.WriteLine("==================================");

        try
        {
            // Uncomment the following line to enable TransactionScope support
            // using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                var client = new MongoClient(ConnectionString);
                var database = client.GetDatabase(DatabaseName);

                var usersCollection = database.GetCollection<User>("users");
                var ordersCollection = database.GetCollection<Order>("orders");

                Console.WriteLine("Starting transactional insert/update...");

                await PerformTransactionalInsertAsync(client, usersCollection, ordersCollection);

                Console.WriteLine("\nVerifying data after transaction:");
                await DisplayCollectionDataAsync(usersCollection, ordersCollection);

                Console.WriteLine("\n" + new string('=', 50));
                Console.WriteLine("Demonstrating failed transaction (will rollback):");
                await DemonstrateFailedTransactionAsync(client, usersCollection, ordersCollection);

                Console.WriteLine("\nVerifying data after failed transaction (should be unchanged):");
                await DisplayCollectionDataAsync(usersCollection, ordersCollection);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            if (ex.InnerException != null)
            {
                Console.WriteLine($"Inner Exception: {ex.InnerException.Message}");
            }
        }

        Console.WriteLine("\nPress any key to exit...");
        Console.ReadKey();
    }

    static async Task PerformTransactionalInsertAsync(MongoClient client, IMongoCollection<User> usersCollection, IMongoCollection<Order> ordersCollection)
    {
        using var session = await client.StartSessionAsync();

        try
        {
            session.StartTransaction();

            // Try to find existing user by email
            var userEmail = "john.doe@example.com";
            var userFilter = Builders<User>.Filter.Eq(u => u.Email, userEmail);
            var existingUser = await usersCollection.Find(session, userFilter).FirstOrDefaultAsync();

            User user;
            if (existingUser != null)
            {
                // Update existing user
                var userUpdate = Builders<User>.Update
                    .Set(u => u.Name, "John Doe (Updated)")
                    .Set(u => u.CreatedAt, DateTime.UtcNow);

                await usersCollection.UpdateOneAsync(session, userFilter, userUpdate);
                user = await usersCollection.Find(session, userFilter).FirstOrDefaultAsync();
                Console.WriteLine($"✓ Updated existing user: {user.Name} (ID: {user.Id})");
            }
            else
            {
                // Insert new user
                user = new User
                {
                    Name = "John Doe",
                    Email = userEmail,
                    CreatedAt = DateTime.UtcNow
                };

                await usersCollection.InsertOneAsync(session, user);
                Console.WriteLine($"✓ Inserted new user: {user.Name} (ID: {user.Id})");
            }

            // Check for existing laptop order
            var laptopOrderFilter = Builders<Order>.Filter.And(
                Builders<Order>.Filter.Eq(o => o.UserId, user.Id),
                Builders<Order>.Filter.Eq(o => o.ProductName, "Laptop Computer")
            );
            var existingLaptopOrder = await ordersCollection.Find(session, laptopOrderFilter).FirstOrDefaultAsync();

            if (existingLaptopOrder != null)
            {
                // Update existing laptop order - apply price increase
                var newAmount = 1399.99m;
                var laptopUpdate = Builders<Order>.Update
                    .Set(o => o.Amount, newAmount)
                    .Set(o => o.OrderDate, DateTime.UtcNow);

                await ordersCollection.UpdateOneAsync(session, laptopOrderFilter, laptopUpdate);
                Console.WriteLine($"✓ Updated laptop order: Price changed from ${existingLaptopOrder.Amount} to ${newAmount}");
            }
            else
            {
                // Insert new laptop order
                var laptopOrder = new Order
                {
                    UserId = user.Id,
                    ProductName = "Laptop Computer",
                    Amount = 1299.99m,
                    OrderDate = DateTime.UtcNow
                };

                await ordersCollection.InsertOneAsync(session, laptopOrder);
                Console.WriteLine($"✓ Inserted new laptop order: ${laptopOrder.Amount} (ID: {laptopOrder.Id})");
            }

            // Check for existing mouse order
            var mouseOrderFilter = Builders<Order>.Filter.And(
                Builders<Order>.Filter.Eq(o => o.UserId, user.Id),
                Builders<Order>.Filter.Eq(o => o.ProductName, "Wireless Mouse")
            );
            var existingMouseOrder = await ordersCollection.Find(session, mouseOrderFilter).FirstOrDefaultAsync();

            if (existingMouseOrder != null)
            {
                // Update existing mouse order - apply discount
                var discountedAmount = Math.Round(existingMouseOrder.Amount * 0.8m, 2); // 20% discount
                var mouseUpdate = Builders<Order>.Update
                    .Set(o => o.Amount, discountedAmount)
                    .Set(o => o.OrderDate, DateTime.UtcNow);

                await ordersCollection.UpdateOneAsync(session, mouseOrderFilter, mouseUpdate);
                Console.WriteLine($"✓ Updated mouse order: Applied 20% discount from ${existingMouseOrder.Amount} to ${discountedAmount}");
            }
            else
            {
                // Insert new mouse order
                var mouseOrder = new Order
                {
                    UserId = user.Id,
                    ProductName = "Wireless Mouse",
                    Amount = 49.99m,
                    OrderDate = DateTime.UtcNow
                };

                await ordersCollection.InsertOneAsync(session, mouseOrder);
                Console.WriteLine($"✓ Inserted new mouse order: ${mouseOrder.Amount} (ID: {mouseOrder.Id})");
            }

            // Always add/update a keyboard order using upsert
            var keyboardOrderFilter = Builders<Order>.Filter.And(
                Builders<Order>.Filter.Eq(o => o.UserId, user.Id),
                Builders<Order>.Filter.Eq(o => o.ProductName, "Mechanical Keyboard")
            );

            var keyboardOrder = new Order
            {
                UserId = user.Id,
                ProductName = "Mechanical Keyboard",
                Amount = 129.99m,
                OrderDate = DateTime.UtcNow
            };

            var upsertOptions = new ReplaceOptions { IsUpsert = true };
            var keyboardResult = await ordersCollection.ReplaceOneAsync(session, keyboardOrderFilter, keyboardOrder, upsertOptions);

            if (keyboardResult.UpsertedId != null)
            {
                Console.WriteLine($"✓ Inserted new keyboard order via upsert: ${keyboardOrder.Amount}");
            }
            else
            {
                Console.WriteLine($"✓ Updated keyboard order via upsert: ${keyboardOrder.Amount}");
            }

            await session.CommitTransactionAsync();
            Console.WriteLine("✓ Transaction committed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Transaction failed: {ex.Message}");
            await session.AbortTransactionAsync();
            Console.WriteLine("✓ Transaction aborted/rolled back");
            throw;
        }
    }

    static async Task DemonstrateFailedTransactionAsync(MongoClient client, IMongoCollection<User> usersCollection, IMongoCollection<Order> ordersCollection)
    {
        using var session = await client.StartSessionAsync();

        try
        {
            session.StartTransaction();

            var user = new User
            {
                Name = "Jane Smith",
                Email = "jane.smith@example.com",
                CreatedAt = DateTime.UtcNow
            };

            await usersCollection.InsertOneAsync(session, user);
            Console.WriteLine($"✓ Inserted user: {user.Name} (ID: {user.Id})");

            var order = new Order
            {
                UserId = user.Id,
                ProductName = "Tablet",
                Amount = 599.99m,
                OrderDate = DateTime.UtcNow
            };

            await ordersCollection.InsertOneAsync(session, order);
            Console.WriteLine($"✓ Inserted order: {order.ProductName} for ${order.Amount} (ID: {order.Id})");

            Console.WriteLine("Simulating business logic error...");
            throw new InvalidOperationException("Simulated business logic failure");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Transaction failed: {ex.Message}");
            await session.AbortTransactionAsync();
            Console.WriteLine("✓ Transaction aborted - all changes rolled back");
        }
    }

    static async Task DisplayCollectionDataAsync(IMongoCollection<User> usersCollection, IMongoCollection<Order> ordersCollection)
    {
        var users = await usersCollection.Find(FilterDefinition<User>.Empty).ToListAsync();
        Console.WriteLine($"\nUsers in database: {users.Count}");
        foreach (var user in users)
        {
            Console.WriteLine($"  - {user.Name} ({user.Email}) - ID: {user.Id}");
        }

        var orders = await ordersCollection.Find(FilterDefinition<Order>.Empty).ToListAsync();
        Console.WriteLine($"\nOrders in database: {orders.Count}");
        foreach (var order in orders)
        {
            Console.WriteLine($"  - {order.ProductName}: ${order.Amount} for User ID: {order.UserId}");
        }
    }
}
