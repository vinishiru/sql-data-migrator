using Microsoft.Data.SqlClient;
using SQLDataMigrator.Descriptors;
using SQLDataMigrator.Extensions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace SQLDataMigrator.Executors
{
  class RegisterInsertor
  {
    private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

    private readonly string connectionString;
    private readonly string tableName;
    private readonly TableDescriptor tableDescriptor;

    public RegisterInsertor(string connectionString, string tableName, TableDescriptor tableDescriptor)
    {
      this.connectionString = connectionString;
      this.tableName = tableName;
      this.tableDescriptor = tableDescriptor;
    }

    public void InserirETruncarRegistros(List<ExpandoObject> registers)
    {
      try
      {
        log.Warn($"Inserindo lote de {registers.Count} na tabela '{tableName}'...");

        var sql = new StringBuilder();

        using (var sqlConnection = new SqlConnection(connectionString))
        {
          sqlConnection.Open();
          foreach (var obj in registers)
          {
            var command = MontarSqlInsertCommand(sqlConnection, obj);
            var rows = command.ExecuteNonQuery();
          }
        }
        //truncar lista
        log.Warn($"Inserção de lote de {registers.Count} na tabela '{tableName}' concluída!");
        registers.Clear();
      }
      catch (Exception ex)
      {
        log.Error($"Ocorreu um erro ao tentar realizar a inserção em lote na tabela {tableName}", ex);
      }
    }

    private SqlCommand MontarSqlInsertCommand(SqlConnection sqlConnection, ExpandoObject obj)
    {
      var colunasInseriveis = tableDescriptor.RecuperarColunasTabela()
        .Where(m => !m.IsComputed)
        .Select(m => m.Name);

      var sqlCommand = new SqlCommand();
      sqlCommand.Connection = sqlConnection;
      sqlCommand.CommandText = $"INSERT INTO {tableName} ({string.Join(",", colunasInseriveis)}) VALUES ( {string.Join(",", colunasInseriveis.Select(m => $"@p_{m}"))} )";

      foreach (var coluna in colunasInseriveis)
        sqlCommand.Parameters.AddWithValue($"@p_{coluna}", obj.RecuperarValorColuna(coluna) ?? DBNull.Value);

      return sqlCommand;
    }

    private string MontarValues(IEnumerable<string> colunasInseriveis, ExpandoObject obj)
    {
      var valores = new List<object>();
      foreach (var coluna in colunasInseriveis)
        valores.Add(obj.RecuperarValorColuna(coluna));

      return $"({string.Join(",", valores)})";
    }
  }
}
