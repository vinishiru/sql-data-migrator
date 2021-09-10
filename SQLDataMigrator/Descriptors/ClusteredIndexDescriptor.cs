using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Linq;
using System.Text;

namespace SQLDataMigrator.Descriptors
{
  public class ClusteredIndexDescriptor : IDisposable
  {
    private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);


    private readonly SqlConnection connection;
    private readonly string tableName;
    private string[] clusteredKeys;

    public class RegistroPkClustered
    {
      public string IndexName { get; set; }
      public string ColumnName { get; set; }
      public int ColumnId { get; set; }
      public byte KeyOrdinal { get; set; }
      public bool IsIncludedColumn { get; set; }
    }

    public ClusteredIndexDescriptor(string connectionString, string tableName)
    {
      this.connection = new SqlConnection(connectionString);
      this.tableName = tableName;
    }

    public string[] RecuperarColunasPkClusterizada()
    {
      if (clusteredKeys != null)
        return clusteredKeys;

      try
      {
        var queryClusteredIndex = $@"SELECT i.name AS IndexName  
                                  ,COL_NAME(ic.object_id,ic.column_id) AS ColumnName  
                                  ,ic.index_column_id  as ColumnId
                                  ,ic.key_ordinal as KeyOrdinal
                                  ,ic.is_included_column  as IsIncludedColumn
                              FROM sys.indexes AS i  
                              INNER JOIN sys.index_columns AS ic
                                  ON i.object_id = ic.object_id AND i.index_id = ic.index_id  
                              WHERE i.object_id = OBJECT_ID('{tableName}')
                              and i.type = 1
                              order by ic.key_ordinal";

        using (var context = new DataContext(connection))
        {
          var colunas = context.ExecuteQuery<RegistroPkClustered>(queryClusteredIndex);
          return clusteredKeys = colunas.Select(m => m.ColumnName).ToArray();
        }
      }
      catch (Exception ex)
      {
        log.Error($"Ocorreu um erro ao tentar obter as colunas da chave clusterizada da tabela '{tableName}'.", ex);
        throw;
      }
    }

    public string RecuperarValorChaveClusterizada(IDictionary<string, object> objeto)
    {
      return $@"{string.Join(" | ",
        objeto
        .Where(m => RecuperarColunasPkClusterizada().Contains(m.Key))
        .Select(m => $"{m.Key}={m.Value}")
        )}";
    }

    public void Dispose()
    {
      if(connection != null)
      {
        connection.Close();
        connection.Dispose();
      }
    }
  }
}
