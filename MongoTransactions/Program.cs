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

                await usersCollection.DeleteManyAsync(FilterDefinition<User>.Empty);
                await ordersCollection.DeleteManyAsync(FilterDefinition<Order>.Empty);

                Console.WriteLine("Starting transactional insert...");

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

            var user = new User
            {
                Name = "John Doe",
                Email = "john.doe@example.com",
                CreatedAt = DateTime.UtcNow
            };

            await usersCollection.InsertOneAsync(session, user);
            Console.WriteLine($"✓ Inserted user: {user.Name} (ID: {user.Id})");

            var order = new Order
            {
                UserId = user.Id,
                ProductName = "Laptop Computer",
                Amount = 1299.99m,
                OrderDate = DateTime.UtcNow
            };

            await ordersCollection.InsertOneAsync(session, order);
            Console.WriteLine($"✓ Inserted order: {order.ProductName} for ${order.Amount} (ID: {order.Id})");

            var order2 = new Order
            {
                UserId = user.Id,
                ProductName = "Wireless Mouse",
                Amount = 49.99m,
                OrderDate = DateTime.UtcNow
            };

            await ordersCollection.InsertOneAsync(session, order2);
            Console.WriteLine($"✓ Inserted order: {order2.ProductName} for ${order2.Amount} (ID: {order2.Id})");

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
