using Microsoft.Data.SqlClient;
using SQLDataMigrator.Descriptors;
using SQLDataMigrator.Extensions;
using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataMigrator.Executors
{
  class RegisterFinderByClusteredIndex : IDisposable
  {
    private readonly SqlConnection sqlConnection;
    private readonly ClusteredIndexDescriptor clusteredIndexDescriptor;
    private readonly string tableName;

    public RegisterFinderByClusteredIndex(string connectionString, ClusteredIndexDescriptor clusteredIndexDescriptor, string tableName)
    {
      this.sqlConnection = new SqlConnection(connectionString);
      this.clusteredIndexDescriptor = clusteredIndexDescriptor;
      this.tableName = tableName;
    }

    public void Dispose()
    {
      if (sqlConnection != null)
      {
        sqlConnection.Close();
        sqlConnection.Dispose();
      }
    }

    public bool ExisteRegistroNaTabela(IDictionary<string, object> obj)
    {
      var keyColumns = clusteredIndexDescriptor.RecuperarColunasPkClusterizada();

      var query = $"SELECT TOP 1 1 FROM {tableName} WHERE 1=1";
      query += string.Concat(keyColumns.Select(m => $" AND {m} = @p_{m} "));

      var sqlCommand = new SqlCommand(query, sqlConnection);

      //montar WHERE
      foreach (var key in keyColumns)
        sqlCommand.Parameters.AddWithValue($"p_{key}", obj.RecuperarValorColuna(key));

      if (sqlConnection.State != System.Data.ConnectionState.Open)
        sqlConnection.Open();

      return (int?)sqlCommand.ExecuteScalar() == 1;

    }


  }
}
