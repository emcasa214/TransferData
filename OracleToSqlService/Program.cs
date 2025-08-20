
using Oracle.ManagedDataAccess.Client;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        string oracleConnStr = "User Id=BIDV_AMCD_NEW;Password=123;Data Source=localhost:1521/xepdb1;";
        string sqlConnStr = "Server=DESKTOP-CT2CAIU\\SQLEXPRESS;Database=BIDV_DB;Integrated Security=true;TrustServerCertificate=true;";

        using var oracleConn = new OracleConnection(oracleConnStr);
        await oracleConn.OpenAsync();
        Console.WriteLine("Kết nối Oracle DB thành công");

        using var sqlConn = new SqlConnection(sqlConnStr);
        await sqlConn.OpenAsync();
        Console.WriteLine("Kết nối SQL Server DB thành công");

        int batchSize = 100;
        long lastId = await GetLastIdFromSqlAsync(sqlConn);
        Console.WriteLine($"Lấy lastId: {lastId} từ SQL Server");

        bool hasMore = true;

        while (hasMore)
        {
            try
            {
                // var sw = Stopwatch.StartNew();

                DataTable dt = await ExtractBatchAsync(oracleConn, lastId, batchSize);

                if (dt.Rows.Count > 0)
                {
                    await LoadBatchAsync(sqlConn, dt);
                    lastId = (long)dt.Rows[^1]["SendRequestId"];
                    Console.WriteLine($"Đã chèn {dt.Rows.Count} bản ghi, lastId mới: {lastId}");
                }
                // sw.Stop();
                // Console.WriteLine($"Batch xử lý mất {sw.ElapsedMilliseconds} ms\n");

                hasMore = dt.Rows.Count == batchSize;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Lỗi batch tại lastId={lastId}: {ex.Message}");
                // break;
            }
        }
    }

    static async Task<long> GetLastIdFromSqlAsync(SqlConnection conn)
    {
        string query = "SELECT TOP 1 SourceId FROM dbo.BIDV_InteractionService ORDER BY SourceId DESC";
        using var cmd = new SqlCommand(query, conn);
        cmd.CommandTimeout = 300;
        var result = await cmd.ExecuteScalarAsync();
        return (result == null || result == DBNull.Value) ? 0 : Convert.ToInt64(result);
    }

    static async Task<DataTable> ExtractBatchAsync(OracleConnection conn, long lastId, int batchSize)
    {
        string batchQuery = @"
            SELECT SEND_REQUEST_ID as SendRequestId, 
                   EMAIL_FROM as EmailFrom,
                   EMAIL_TO,
                   REQUEST_TIME,
                   AGENT_PROCESS
            FROM EMAIL_SEND_REQUEST
            WHERE SEND_REQUEST_ID > :lastId
            ORDER BY SEND_REQUEST_ID
            FETCH NEXT :batchSize ROWS ONLY
        ";

        using var cmd = new OracleCommand(batchQuery, conn);
        cmd.BindByName = true;
        cmd.Parameters.Add(new OracleParameter("lastId", lastId));
        cmd.Parameters.Add(new OracleParameter("batchSize", batchSize));

        using var reader = await cmd.ExecuteReaderAsync();

        var dt = new DataTable();
        dt.Columns.Add("Id", typeof(Guid));
        dt.Columns.Add("SendRequestId", typeof(long));
        dt.Columns.Add("EmailFrom", typeof(string));
        dt.Columns.Add("EmailTo", typeof(string));
        dt.Columns.Add("RequestTime", typeof(DateTime));
        dt.Columns.Add("AgentProcess", typeof(string));
        dt.Columns.Add("ReceiverChannel", typeof(int));

        while (await reader.ReadAsync())
        {
            var row = dt.NewRow();
            row["Id"] = Guid.NewGuid();
            row["SendRequestId"] = reader.IsDBNull(0) ? DBNull.Value : reader.GetInt64(0);
            row["EmailFrom"] = reader.IsDBNull(1) ? DBNull.Value : reader.GetString(1);
            row["EmailTo"] = reader.IsDBNull(2) ? DBNull.Value : reader.GetString(2);
            row["RequestTime"] = reader.IsDBNull(3) ? DBNull.Value : reader.GetDateTime(3);
            row["AgentProcess"] = reader.IsDBNull(4) ? DBNull.Value : reader.GetString(4);
            row["ReceiverChannel"] = 1;
            dt.Rows.Add(row);
        }

        return dt;
    }

    static async Task LoadBatchAsync(SqlConnection conn, DataTable dt)
    {
        using var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null)
        {
            DestinationTableName = "dbo.BIDV_InteractionService",
            BulkCopyTimeout = 300
        };

        bulkCopy.ColumnMappings.Add("Id", "Id");
        bulkCopy.ColumnMappings.Add("SendRequestId", "SourceId");
        bulkCopy.ColumnMappings.Add("EmailFrom", "ContactFrom");
        bulkCopy.ColumnMappings.Add("EmailTo", "ContactReceive");
        bulkCopy.ColumnMappings.Add("RequestTime", "Receive_Date");
        bulkCopy.ColumnMappings.Add("AgentProcess", "Recipient");
        bulkCopy.ColumnMappings.Add("ReceiverChannel", "ReceiverChannel");

        await bulkCopy.WriteToServerAsync(dt);
    }
}
