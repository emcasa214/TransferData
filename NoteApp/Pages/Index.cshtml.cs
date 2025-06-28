using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Oracle.ManagedDataAccess.Client;
using NoteApp.Models;
using System.Diagnostics;

namespace NoteApp.Pages;

public class IndexModel : PageModel
{
    public List<Note> Notes = new();

    [BindProperty]
    public string NewNote { get; set; }= string.Empty;

    private readonly string connStr = "User Id=note_user;Password=123;Data Source=localhost:1521/xepdb1;";

    public void OnGet()
    {
        try
        {
            using var conn = new OracleConnection(connStr);
            conn.Open();
            Console.WriteLine("Kết nối Oracle DB thành công");

            var cmd = new OracleCommand("SELECT * FROM notes", conn);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                Notes.Add(new Note
                {
                    Id = reader.GetInt32(0),
                    Content = reader.IsDBNull(1) ? "" : reader.GetString(1)
                });
            }

            Console.WriteLine($"Đọc dữ liệu thành công, có {Notes.Count} dòng");
        }
        catch (OracleException ex)
        {
            Console.WriteLine($"OracleException: {ex.Message}");
            throw;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception: {ex.Message}");
            throw;
        }
    }
    public IActionResult OnPost()
    {
        using var conn = new OracleConnection(connStr);
        conn.Open();

        var cmd = new OracleCommand("INSERT INTO notes (note) VALUES (:note)", conn);
        cmd.Parameters.Add(new OracleParameter("note", NewNote));
        cmd.ExecuteNonQuery();
        return RedirectToPage();
    }
}   
