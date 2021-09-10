using Microsoft.Data.SqlClient;
using SQLDataMigrator.Descriptors;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SQLDataMigrator
{
  public class TableCursor : IDisposable
  {
    private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

    private static object _locker = new object();
    private static TableCursor _instance;
    private static string _currentTableName;
    private readonly SqlConnection sqlConnection;
    private readonly TableDescriptor tableDescriptor;
    private readonly string tableName;
    private readonly string filtro;
    private SqlDataReader reader;
    private long? totalRegistros;
    private SemaphoreSlim semaphoreSlim;
    private ConcurrentQueue<ExpandoObject> cacheObjetos;

    private TableCursor(string connectionString, string tableName, string filtro)
    {
      this.sqlConnection = new SqlConnection(connectionString);
      this.tableDescriptor = new TableDescriptor(connectionString, tableName);
      this.tableName = tableDescriptor.RecuperarNomeCompletoTabela();
      this.filtro = filtro;
      this.semaphoreSlim = new SemaphoreSlim(1, 1);
      this.cacheObjetos = new ConcurrentQueue<ExpandoObject>();
    }

    public static TableCursor GetInstance(string connectionString, string tableName, string filtro)
    {
      if (_currentTableName == tableName && _instance != null)
        return _instance;

      lock (_locker)
      {
        if (_currentTableName == tableName && _instance != null)
          return _instance;

        if (_instance != null)
          _instance.Dispose();

        _currentTableName = tableName;
        return _instance = new TableCursor(connectionString, tableName, filtro);
      }
    }

    public void Dispose()
    {
      if (reader != null)
      {
        reader.Close();
        reader.Dispose();
      }

      if (sqlConnection != null)
      {
        sqlConnection.Close();
        sqlConnection.Dispose();
      }
    }

    public ExpandoObject[] RecuperarProximosObjetos(int quantidade = 1)
    {
      if (RecuperarLoteObjetosCache(quantidade, out ExpandoObject[] listaCache))
        return listaCache;

      semaphoreSlim.Wait();

      try
      {
        //verificar novamente se o cache está preenchido
        if (RecuperarLoteObjetosCache(quantidade, out listaCache))
          return listaCache;

        //se chegou aqui, temos que alimentar o cache
        PreencherCacheObjetos(2000);

        if (RecuperarLoteObjetosCache(quantidade, out listaCache))
          return listaCache;
      }
      finally
      {
        semaphoreSlim.Release();
      }

      return Enumerable.Empty<ExpandoObject>().ToArray();
    }

    private void PreencherCacheObjetos(int quantidadeItensCache)
    {
      var reader = IniciarLeitura();
      var cont = 0;

      if (reader.IsClosed)
        return;

      while (cont < quantidadeItensCache && reader.Read())
      {
        var objeto = new ExpandoObject() as IDictionary<string, object>;
        var colunas = tableDescriptor.RecuperarColunasTabela().Select(m => m.Name).ToArray();

        for (int i = 0; i < reader.FieldCount; i++)
          objeto.Add(colunas[i], reader[i] is DBNull ? null : reader[i]);

        if (cont % 100 == 0)
          log.Warn($"Alimentando cache {cont}...");

        cacheObjetos.Enqueue(objeto as ExpandoObject);
        cont++;
      }
    }

    private bool RecuperarLoteObjetosCache(int quantidade, out ExpandoObject[] listaCache)
    {
      listaCache = Enumerable.Empty<ExpandoObject>().ToArray();

      if (!cacheObjetos.Any() || !cacheObjetos.TryPeek(out ExpandoObject peek))
        return false;

      var resultado = new List<ExpandoObject>();

      for (var i = 0; i < quantidade && cacheObjetos.TryDequeue(out ExpandoObject objeto); i++)
        resultado.Add(objeto);

      listaCache = resultado.ToArray();
      return true;
    }

    public long? ContabilizarRegistros()
    {
      if (totalRegistros.HasValue)
        return totalRegistros.Value;

      var colunas = tableDescriptor.RecuperarColunasTabela().Select(m => m.Name);

      var command = new SqlCommand($"exec sp_spaceused '{tableName}'", sqlConnection);
      command.CommandTimeout = 0;

      if (!string.IsNullOrEmpty(filtro))
        command.CommandText += filtro;

      if (sqlConnection.State != System.Data.ConnectionState.Open)
        sqlConnection.Open();

      using (var reader = command.ExecuteReader())
      {
        if (!reader.Read())
          return 0;

        return totalRegistros = long.Parse(reader.GetString(1));
      }
    }

    private SqlDataReader IniciarLeitura()
    {
      if (reader != null)
        return reader;

      var colunas = tableDescriptor.RecuperarColunasTabela().Select(m => m.Name);

      var command = new SqlCommand($"select {string.Join(",", colunas)} from {tableName}", sqlConnection);

      if (!string.IsNullOrEmpty(filtro))
        command.CommandText += filtro;

      if (sqlConnection.State != System.Data.ConnectionState.Open)
        sqlConnection.Open();

      return reader = command.ExecuteReader();
    }
  }
}
