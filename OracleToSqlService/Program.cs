using Oracle.ManagedDataAccess.Client;
using Microsoft.Data.SqlClient;

string oracleConnStr = "User Id=note_user;Password=123;Data Source=localhost:1521/xepdb1;";
string sqlConnStr = "Server=DESKTOP-CT2CAIU\\SQLEXPRESS;Database=NoteDb;Integrated Security=True;TrustServerCertificate=True;";

using var oracleConn = new OracleConnection(oracleConnStr);
oracleConn.Open();
Console.WriteLine("Kết nối Oracle DB thành công");

using var sqlConn = new SqlConnection(sqlConnStr);
sqlConn.Open();
Console.WriteLine("Kết nối SQL Server DB thành công");

using var sqlCmd = new SqlCommand("SELECT ISNULL(MAX(id), 0) FROM notes", sqlConn);
int maxId = (int)sqlCmd.ExecuteScalar();

using var oracleCmd = new OracleCommand("SELECT * FROM notes WHERE id > :maxId", oracleConn);
oracleCmd.Parameters.Add(new OracleParameter("maxId", maxId));
using var read = oracleCmd.ExecuteReader();
while (read.Read())
{
    int id = read.GetInt32(0);
    string note = read.IsDBNull(1) ? "" : read.GetString(1);

    using var insertCmd = new SqlCommand("INSERT INTO notes (id,note) VALUES (@id, @note)", sqlConn);
    insertCmd.Parameters.AddWithValue("@id", id);
    insertCmd.Parameters.AddWithValue("@note", note);
    insertCmd.ExecuteNonQuery();
    maxId = id;
    Console.WriteLine($"Chèn thành công: {id} - {note}");
}
Thread.Sleep(TimeSpan.FromMinutes(1));