using Oracle.ManagedDataAccess.Client;
using Microsoft.Data.SqlClient;

string oracleConnStr = "User Id=note_user;Password=123;Data Source=localhost:1521/xepdb1;";
string sqlConnStr = "Server=10.1.6.35;Database=Test;User Id=ucx;Password=Sunivy111!;TrustServerCertificate=True;";

using var oracleConn = new OracleConnection(oracleConnStr);
oracleConn.Open();
Console.WriteLine("Kết nối Oracle DB thành công");

using var sqlConn = new SqlConnection(sqlConnStr);
sqlConn.Open();
Console.WriteLine("Kết nối SQL Server DB thành công");

while (true)
{
    using var sqlCmd = new SqlCommand("SELECT ISNULL(MAX(id), 0) FROM notes", sqlConn);
    int maxId = (int)sqlCmd.ExecuteScalar();

    int batchSize = 10000; // Số lượng bản ghi mỗi batch
    bool hasMore = true;
    while (hasMore)
    {
        // Đọc batch từ Oracle
        string batchQuery = $@"SELECT * FROM notes WHERE id > :maxId AND ROWNUM <= :batchSize ORDER BY id";
        using var oracleCmd = new OracleCommand(batchQuery, oracleConn);
        oracleCmd.Parameters.Add(new OracleParameter("maxId", maxId));
        oracleCmd.Parameters.Add(new OracleParameter("batchSize", batchSize));
        using var reader = oracleCmd.ExecuteReader();

        var dt = new System.Data.DataTable();
        dt.Columns.Add("id", typeof(int));
        dt.Columns.Add("note", typeof(string));

        int count = 0;
        int lastId = maxId;
        while (reader.Read())
        {
            int id = reader.GetInt32(0);
            string note = reader.IsDBNull(1) ? "" : reader.GetString(1);
            dt.Rows.Add(id, note);
            lastId = id;
            count++;
        }

        if (count > 0)
        {
            using (var bulkCopy = new SqlBulkCopy(sqlConn))
            {
                bulkCopy.DestinationTableName = "notes";
                bulkCopy.ColumnMappings.Add("id", "id");
                bulkCopy.ColumnMappings.Add("note", "note");
                bulkCopy.WriteToServer(dt);
            }
            Console.WriteLine($"Bulk insert {count} bản ghi thành công!");
            maxId = lastId;
        }

        hasMore = count == batchSize;
    }
    Console.WriteLine("Đã chuyển xong batch, đợi 1 phút...");
    Thread.Sleep(TimeSpan.FromMinutes(1));
}



//code 2
using Oracle.ManagedDataAccess.Client;
using Microsoft.Data.SqlClient;

string oracleConnStr = "User Id=BIDV_AMCD_NEW;Password=123;Data Source=localhost:1521/xepdb1;";
string sqlConnStr = "Server=DESKTOP-CT2CAIU\\SQLEXPRESS;Database=BIDV_DB;Integrated Security=true;TrustServerCertificate=true;";
//private: "Server=DESKTOP-CT2CAIU\\SQLEXPRESS;Database=BIDV_DB;Integrated Security=true;TrustServerCertificate=true;";
//lab: "Server=10.1.6.35;Database=Test;User Id=ucx;Password=Sunivy111!;TrustServerCertificate=True;";

using var oracleConn = new OracleConnection(oracleConnStr);
oracleConn.Open();
Console.WriteLine("Kết nối Oracle DB thành công");

using var sqlConn = new SqlConnection(sqlConnStr);
sqlConn.Open();
Console.WriteLine("Kết nối SQL Server DB thành công");

int batchSize = 1000;
long lastId = 0;
string query = "SELECT TOP 1 SourceId FROM dbo.BIDV_InteractionService ORDER BY SourceId DESC";
using (var sqlCmd = new SqlCommand(query, sqlConn))
{
    sqlCmd.CommandTimeout = 300;  
    var result = sqlCmd.ExecuteScalar();
    if (result == null || result == DBNull.Value)
        lastId = 0;
    else
        lastId = Convert.ToInt64(result);
}
Console.WriteLine($"Lấy lastId: {lastId} từ SQL Server");

bool hasMore = true;

while (hasMore)
{
    //extract
    string batchQuery = @"
        SELECT newid() as Id,
                SEND_REQUEST_ID as SendRequestId, 
               EMAIL_FROM as EmailFrom,
               EMAIL_TO,
               REQUEST_TIME,
               AGENT_PROCESS
        FROM EMAIL_SEND_REQUEST
        WHERE SEND_REQUEST_ID > :lastId
        ORDER BY SEND_REQUEST_ID
        FETCH NEXT :batchSize ROWS ONLY
    ";
    using var oracleCmd = new OracleCommand(batchQuery, oracleConn);
    oracleCmd.Parameters.Add(new OracleParameter("lastId", lastId));
    oracleCmd.Parameters.Add(new OracleParameter("batchSize", batchSize));
    using var reader = oracleCmd.ExecuteReader();

    // if (!reader.HasRows)
    // {
    //     Console.WriteLine("chưa lấy được dữ liệu");
    // }
    // else
    // {
    //     Console.WriteLine("Đã lấy được dữ liệu");
    // }

    //transform
    var dt = new System.Data.DataTable();
    dt.Columns.Add("Id", typeof(Guid));
    dt.Columns.Add("SendRequestId", typeof(long));
    dt.Columns.Add("EmailFrom", typeof(string));
    dt.Columns.Add("EmailTo", typeof(string));
    dt.Columns.Add("RequestTime", typeof(DateTime));
    dt.Columns.Add("AgentProcess", typeof(string));
    dt.Columns.Add("ReceiverChannel", typeof(int));

    int cnt = 0;
    while (reader.Read())
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
        lastId = reader.GetInt64(0);
        cnt++;
    }

    //load
    if (cnt > 0)
    {
        using var sqlBulkCopy = new SqlBulkCopy(sqlConn,SqlBulkCopyOptions.TableLock,null);
        sqlBulkCopy.DestinationTableName = "dbo.BIDV_InteractionService";
        sqlBulkCopy.BulkCopyTimeout = 300; 
        sqlBulkCopy.ColumnMappings.Add("Id", "Id");
        sqlBulkCopy.ColumnMappings.Add("SendRequestId", "SourceId");
        sqlBulkCopy.ColumnMappings.Add("EmailFrom", "ContactFrom");
        sqlBulkCopy.ColumnMappings.Add("EmailTo", "ContactReceive");
        sqlBulkCopy.ColumnMappings.Add("RequestTime", "Receive_Date");
        sqlBulkCopy.ColumnMappings.Add("AgentProcess", "Recipient");
        sqlBulkCopy.ColumnMappings.Add("ReceiverChannel",  "ReceiverChannel");

        sqlBulkCopy.WriteToServer(dt);
        Console.WriteLine($"Đã chèn {cnt} bản ghi từ id {lastId} vào SQL Server");
    }
    hasMore = cnt == batchSize;
}

<!-- code 3 -->
using Oracle.ManagedDataAccess.Client;
using Microsoft.Data.SqlClient;
using System.Data;

string oracleConnStr = "User Id=BIDV_AMCD_NEW;Password=123;Data Source=localhost:1521/xepdb1;";
string sqlConnStr = "Server=DESKTOP-CT2CAIU\\SQLEXPRESS;Database=BIDV_DB;Integrated Security=true;TrustServerCertificate=true;";
//private: "Server=DESKTOP-CT2CAIU\\SQLEXPRESS;Database=BIDV_DB;Integrated Security=true;TrustServerCertificate=true;";
//lab: "Server=10.1.6.35;Database=Test;User Id=ucx;Password=Sunivy111!;TrustServerCertificate=True;";

using var oracleConn = new OracleConnection(oracleConnStr);
oracleConn.Open();
Console.WriteLine("Kết nối Oracle DB thành công");

using var sqlConn = new SqlConnection(sqlConnStr);
sqlConn.Open();
Console.WriteLine("Kết nối SQL Server DB thành công");

int batchSize = 10;
long lastId =  GetLastIdFromSql(sqlConn);
bool hasMore = true;

Console.WriteLine($"Lấy lastId: {lastId} từ SQL Server");

while (hasMore)
{
    //extract + transform
    string batchQuery = @"
        SELECT SYS_GUID() as Id,
               SEND_REQUEST_ID as SendRequestId, 
               EMAIL_FROM as EmailFrom,
               EMAIL_TO as EmailTo,
               REQUEST_TIME as RequestTime,
               AGENT_PROCESS as AgentProcess,
               1 as ReceiverChannel
        FROM EMAIL_SEND_REQUEST
        WHERE SEND_REQUEST_ID > :lastId
        ORDER BY SEND_REQUEST_ID
        FETCH NEXT :batchSize ROWS ONLY
    ";
    using var oracleCmd = new OracleCommand(batchQuery, oracleConn);
    oracleCmd.Parameters.Add(new OracleParameter("lastId", lastId));
    oracleCmd.Parameters.Add(new OracleParameter("batchSize", batchSize));
    using var reader = oracleCmd.ExecuteReader();

    var table = new DataTable();
    table.Load(reader);
    // Convert byte[] → Guid
    foreach (DataRow row in table.Rows)
    {
        if (row["Id"] is byte[] raw && raw.Length == 16)
            row["Id"] = new Guid(raw);
    }

    int rowCount = 0;
    long maxSourceId = lastId;

    //load
    using (var sqlBulk = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.TableLock, null))
    {
        sqlBulk.DestinationTableName = "dbo.BIDV_InteractionService";
        sqlBulk.BulkCopyTimeout = 300;
        sqlBulk.ColumnMappings.Add("Id", "Id");
        sqlBulk.ColumnMappings.Add("SendRequestId", "SourceId");
        sqlBulk.ColumnMappings.Add("EmailFrom", "ContactFrom");
        sqlBulk.ColumnMappings.Add("EmailTo", "ContactReceive");
        sqlBulk.ColumnMappings.Add("RequestTime", "Receive_Date");
        sqlBulk.ColumnMappings.Add("AgentProcess", "Recipient");
        sqlBulk.ColumnMappings.Add("ReceiverChannel", "ReceiverChannel");

        sqlBulk.WriteToServer(table);
    }

    reader.Close();
    using (var maxCmd = new OracleCommand(
        "SELECT MAX(SEND_REQUEST_ID) FROM EMAIL_SEND_REQUEST WHERE SEND_REQUEST_ID > :lastId AND ROWNUM <= :batchSize",
        oracleConn))
    {
        maxCmd.Parameters.Add(new OracleParameter("lastId", lastId));
        maxCmd.Parameters.Add(new OracleParameter("batchSize", batchSize));
        var result = maxCmd.ExecuteScalar();
        if (result != DBNull.Value && result != null)
            maxSourceId = Convert.ToInt64(result);
    }

    rowCount = (int)(maxSourceId - lastId);
    lastId = maxSourceId;
    Console.WriteLine($"Đã chèn khoảng {rowCount} bản ghi, lastId = {lastId}");

    hasMore = rowCount >= batchSize;
}

static long GetLastIdFromSql(SqlConnection sqlConn)
{
    string query = "SELECT TOP 1 SourceId FROM dbo.BIDV_InteractionService ORDER BY SourceId DESC";
    using var sqlCmd = new SqlCommand(query, sqlConn) { CommandTimeout = 300 };
    var result = sqlCmd.ExecuteScalar();
    return (result == null || result == DBNull.Value) ? 0 : Convert.ToInt64(result);
}

<!-- code 4 -->
using Oracle.ManagedDataAccess.Client;
using Microsoft.Data.SqlClient;

string oracleConnStr = "User Id=BIDV_AMCD_NEW;Password=123;Data Source=localhost:1521/xepdb1;";
string sqlConnStr = "Server=DESKTOP-CT2CAIU\\SQLEXPRESS;Database=BIDV_DB;Integrated Security=true;TrustServerCertificate=true;";
//private: "Server=DESKTOP-CT2CAIU\\SQLEXPRESS;Database=BIDV_DB;Integrated Security=true;TrustServerCertificate=true;";
//lab: "Server=10.1.6.35;Database=Test;User Id=ucx;Password=Sunivy111!;TrustServerCertificate=True;";

using var oracleConn = new OracleConnection(oracleConnStr);
oracleConn.Open();
Console.WriteLine("Kết nối Oracle DB thành công");

using var sqlConn = new SqlConnection(sqlConnStr);
sqlConn.Open();
Console.WriteLine("Kết nối SQL Server DB thành công");

int batchSize = 1000;
long lastId = 0;
string query = "SELECT TOP 1 SourceId FROM dbo.BIDV_InteractionService ORDER BY SourceId DESC";
using (var sqlCmd = new SqlCommand(query, sqlConn))
{
    sqlCmd.CommandTimeout = 300;  
    var result = sqlCmd.ExecuteScalar();
    if (result == null || result == DBNull.Value)
        lastId = 0;
    else
        lastId = Convert.ToInt64(result);
}
Console.WriteLine($"Lấy lastId: {lastId} từ SQL Server");

bool hasMore = true;

while (hasMore)
{
    //extract
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
    using var oracleCmd = new OracleCommand(batchQuery, oracleConn);
    oracleCmd.Parameters.Add(new OracleParameter("lastId", lastId));
    oracleCmd.Parameters.Add(new OracleParameter("batchSize", batchSize));
    using var reader = oracleCmd.ExecuteReader();

    // if (!reader.HasRows)
    // {
    //     Console.WriteLine("chưa lấy được dữ liệu");
    // }
    // else
    // {
    //     Console.WriteLine("Đã lấy được dữ liệu");
    // }

    //transform
    var dt = new System.Data.DataTable();
    dt.Columns.Add("Id", typeof(Guid));
    dt.Columns.Add("SendRequestId", typeof(long));
    dt.Columns.Add("EmailFrom", typeof(string));
    dt.Columns.Add("EmailTo", typeof(string));
    dt.Columns.Add("RequestTime", typeof(DateTime));
    dt.Columns.Add("AgentProcess", typeof(string));
    dt.Columns.Add("ReceiverChannel", typeof(int));

    int cnt = 0;
    while (reader.Read())
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
        lastId = reader.GetInt64(0);
        cnt++;
    }

    //load
    if (cnt > 0)
    {
        using var sqlBulkCopy = new SqlBulkCopy(sqlConn,SqlBulkCopyOptions.TableLock,null);
        sqlBulkCopy.DestinationTableName = "dbo.BIDV_InteractionService";
        sqlBulkCopy.BulkCopyTimeout = 300; 
        sqlBulkCopy.ColumnMappings.Add("Id", "Id");
        sqlBulkCopy.ColumnMappings.Add("SendRequestId", "SourceId");
        sqlBulkCopy.ColumnMappings.Add("EmailFrom", "ContactFrom");
        sqlBulkCopy.ColumnMappings.Add("EmailTo", "ContactReceive");
        sqlBulkCopy.ColumnMappings.Add("RequestTime", "Receive_Date");
        sqlBulkCopy.ColumnMappings.Add("AgentProcess", "Recipient");
        sqlBulkCopy.ColumnMappings.Add("ReceiverChannel",  "ReceiverChannel");

        sqlBulkCopy.WriteToServer(dt);
        Console.WriteLine($"Đã chèn {cnt} bản ghi từ id {lastId} vào SQL Server");
    }
    hasMore = cnt == batchSize;
}

<!-- code 5 -->
using Oracle.ManagedDataAccess.Client;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Diagnostics;

string oracleConnStr = "User Id=BIDV_AMCD_NEW;Password=123;Data Source=localhost:1521/xepdb1;";
string sqlConnStr = "Server=DESKTOP-CT2CAIU\\SQLEXPRESS;Database=BIDV_DB;Integrated Security=true;TrustServerCertificate=true;";

using var oracleConn = new OracleConnection(oracleConnStr);
oracleConn.Open();
Console.WriteLine("Kết nối Oracle DB thành công");

using var sqlConn = new SqlConnection(sqlConnStr);
sqlConn.Open();
Console.WriteLine("Kết nối SQL Server DB thành công");

int batchSize = 1000;
long lastId = GetLastIdFromSql(sqlConn);
Console.WriteLine($"Lấy lastId: {lastId} từ SQL Server");

bool hasMore = true;

while (hasMore)
{
    try
    {
        // var sw = Stopwatch.StartNew();

        DataTable dt = ExtractBatch(oracleConn, lastId, batchSize);

        if (dt.Rows.Count > 0)
        {
            LoadBatch(sqlConn, dt);
            lastId = (long)dt.Rows[^1]["SendRequestId"]; // Lấy ID của bản ghi cuối
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

long GetLastIdFromSql(SqlConnection conn)
{
    string query = "SELECT TOP 1 SourceId FROM dbo.BIDV_InteractionService ORDER BY SourceId DESC";
    using var cmd = new SqlCommand(query, conn);
    cmd.CommandTimeout = 300;
    var result = cmd.ExecuteScalar();
    return (result == null || result == DBNull.Value) ? 0 : Convert.ToInt64(result);
}

DataTable ExtractBatch(OracleConnection conn, long lastId, int batchSize)
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

    using var reader = cmd.ExecuteReader();

    var dt = new DataTable();
    dt.Columns.Add("Id", typeof(Guid));
    dt.Columns.Add("SendRequestId", typeof(long));
    dt.Columns.Add("EmailFrom", typeof(string));
    dt.Columns.Add("EmailTo", typeof(string));
    dt.Columns.Add("RequestTime", typeof(DateTime));
    dt.Columns.Add("AgentProcess", typeof(string));
    dt.Columns.Add("ReceiverChannel", typeof(int));

    while (reader.Read())
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

void LoadBatch(SqlConnection conn, DataTable dt)
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

    bulkCopy.WriteToServer(dt);
}
