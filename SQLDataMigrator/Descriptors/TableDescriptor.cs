using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataMigrator.Descriptors
{
  public class TableDescriptor : IDisposable
  {
    private readonly SqlConnection sqlConnection;
    private readonly string tableName;

    private List<RegistroColuna> colunasTabela;

    public class RegistroColuna
    {
      public string Name { get; set; }
      public string Collation { get; set; }
      public bool IsComputed { get; set; }
    }

    public TableDescriptor(string connectionString, string tableName)
    {
      this.sqlConnection = new SqlConnection(connectionString);
      this.tableName = tableName;
    }

    public IEnumerable<RegistroColuna> RecuperarColunasTabela()
    {
      if (colunasTabela != null)
        return colunasTabela;

      var nomeCompletoTabela = RecuperarNomeCompletoTabela();

      if (!ExisteTabela())
        throw new ApplicationException($"A tabela '{nomeCompletoTabela}' não foi encontrada no banco.");

      var sql = $@"SELECT name as Name,
                  collation_name as Collation,
                  is_computed as IsComputed
                  FROM sys.columns WHERE object_id = OBJECT_ID('{nomeCompletoTabela}')
                  order by column_id";

      using (var context = new DataContext(sqlConnection))
      {
        var colunasTabela = context.ExecuteQuery<RegistroColuna>(sql);
        return this.colunasTabela = colunasTabela.ToList();
      }
    }

    private bool ExisteTabela()
    {
      var query = $"select TOP 1 1 from sys.tables where name = '{tableName}'";
      var sqlCommand = new SqlCommand(query, sqlConnection);
      
      if (sqlConnection.State != System.Data.ConnectionState.Open)
        sqlConnection.Open();

      using (var reader = sqlCommand.ExecuteReader())
        return reader.HasRows;
    }

    public string RecuperarNomeCompletoTabela()
    {
      var query = $@"select top 1 s.name + '.' + t.name from sys.tables t
                  join sys.schemas s on s.schema_id = t.schema_id
                  where t.name = '{tableName}'";

      var sqlCommand = new SqlCommand(query, sqlConnection);
      
      if (sqlConnection.State != System.Data.ConnectionState.Open)
        sqlConnection.Open();

      return sqlCommand.ExecuteScalar().ToString();
    }

    public void Dispose()
    {
      if (sqlConnection != null)
      {
        sqlConnection.Close();
        sqlConnection.Dispose();
      }
    }
  }
}
