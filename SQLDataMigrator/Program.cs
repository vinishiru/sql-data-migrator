using log4net;
using log4net.Config;
using Microsoft.Data.SqlClient;
using SQLDataMigrator.Descriptors;
using SQLDataMigrator.Executors;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace SQLDataMigrator
{
  class Program
  {

    private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

    static void Main(string[] args)
    {
      var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
      XmlConfigurator.Configure(logRepository, new FileInfo("log.config"));

      //var sourceConnectionString = "Server=(local);Database=SWorks_Master;Trusted_Connection=True;";
      string sourceConnectionString = @"Data Source=172.16.0.83;Initial Catalog=Tfs_DefaultCollection_Old;Integrated Security=True;Application Name=SQLDataMigrator;TrustServerCertificate=True;Connection Timeout=0";
      string targetConnectionString = @"Data Source=172.16.0.83;Initial Catalog=Tfs_DefaultCollection;Integrated Security=True;Application Name=SQLDataMigrator;TrustServerCertificate=True;Connection Timeout=0";

      try
      {
        string[] tabelas = File.ReadAllLines(@"tabelas.txt");

        foreach (var linha in tabelas)
        {
          var tabela = string.Empty;
          var filtro = string.Empty;
          if (linha.Contains("|"))
          {
            tabela = linha.Split(new string[] { "|" }, StringSplitOptions.RemoveEmptyEntries)[0];
            filtro = linha.Split(new string[] { "|" }, StringSplitOptions.RemoveEmptyEntries)[1];
          }
          else
            tabela = linha;

          log.Info($"Iniciando cópia da tabela {tabela} =============================");
          if (!string.IsNullOrEmpty(filtro))
            log.Info($"Filtro aplicado: '{filtro}'");

          log.Warn($"Base de Origem: {sourceConnectionString}");
          log.Warn($"Base de Destino: {targetConnectionString}");


          //percorrer todos os registros da tabela de origem
          int quantidadeRegistrosProcessados = 0;
          int quantidadeRegistrosInseridos = 0;

          try
          {
            //configurações
            var quantidadeTasks = Environment.ProcessorCount;
            var quantidadeRegistrosPorTask = 100;

            var tasks = new List<Task>();

            //cursor utilizado por todas as tasks para obtenção dos valores
            var cursor = TableCursor.GetInstance(sourceConnectionString, tabela, $" {filtro} ");

            log.Info($"Contabilizando registros na base de origem...");

            var totalRegistros = cursor.ContabilizarRegistros();

            log.Warn($"Quantidade de Tasks:\t{quantidadeTasks}");
            log.Warn($"Quantidade de Registros por Task:\t{quantidadeRegistrosPorTask}");
            log.Warn($"Total de registros:\t{totalRegistros}");

            log.Info("Pressione qualquer tecla para continuar...");
            Console.ReadKey();

            for (var i = 0; i < quantidadeTasks; i++)
            {
              tasks.Add(Task.Factory.StartNew(() =>
              {
                var sourceTableDescriptor = new TableDescriptor(sourceConnectionString, tabela);
                var nomeCompletoTabela = sourceTableDescriptor.RecuperarNomeCompletoTabela();
                var registerInsertor = new RegisterInsertor(targetConnectionString, nomeCompletoTabela, sourceTableDescriptor);

                using (var sourceClusteredKeyDescriptor = new ClusteredIndexDescriptor(sourceConnectionString, nomeCompletoTabela))
                {
                  var listaObjetos = cursor.RecuperarProximosObjetos(quantidadeRegistrosPorTask);
                  var listaObjetosAInserir = new List<ExpandoObject>();

                  while (listaObjetos.Any())
                  {
                    log.Info($"Task {Task.CurrentId} recuperou {listaObjetos?.Count()} registros da tabela '{nomeCompletoTabela}'...");

                    using (var finder = new RegisterFinderByClusteredIndex(targetConnectionString, sourceClusteredKeyDescriptor, nomeCompletoTabela))
                      foreach (var objeto in listaObjetos)
                      {
                        //verificar se existe registro com mesma pk na tabela de destino
                        if (finder.ExisteRegistroNaTabela(objeto))
                        {
                          //log.Debug($"Registro de chave {sourceClusteredKeyDescriptor.RecuperarValorChaveClusterizada(objeto)} da tabela '{nomeCompletoTabela}' já existe no destino.");
                        }
                        else
                        {
                          Interlocked.Increment(ref quantidadeRegistrosInseridos);
                          //inserir registro na tabela de destino
                          //log.Debug($"Inserindo registro de chave {sourceClusteredKeyDescriptor.RecuperarValorChaveClusterizada(objeto)} da tabela '{nomeCompletoTabela}'.");
                          listaObjetosAInserir.Add(objeto);
                        }

                        Interlocked.Increment(ref quantidadeRegistrosProcessados);

                        if (quantidadeRegistrosProcessados % 100 == 0)
                          log.Info($"Tabela: {nomeCompletoTabela}\tProcessados: {quantidadeRegistrosProcessados}\tInseridos: {quantidadeRegistrosInseridos}");

                        //inserção de lote de 100
                        if (listaObjetosAInserir.Any() && listaObjetosAInserir.Count % 100 == 0)
                          registerInsertor.InserirETruncarRegistros(listaObjetosAInserir);

                      }//fim for

                    listaObjetos = cursor.RecuperarProximosObjetos(quantidadeRegistrosPorTask);
                  }//fim while

                  if (listaObjetosAInserir.Any())
                    //inserção final
                    registerInsertor.InserirETruncarRegistros(listaObjetosAInserir);

                }//fim using
              }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default));

            }

            Task.WaitAll(tasks.ToArray());

            if (cursor != null)
              cursor.Dispose();
          }

          catch (ApplicationException ex)
          {
            log.Error(ex.Message);
          }
          catch (Exception ex)
          {
            log.Error($"Ocorreu um erro na cópia da tabela '{tabela}'.", ex);
          }
          log.Info($"Tabela: {tabela}\tProcessados: {quantidadeRegistrosProcessados}\tInseridos: {quantidadeRegistrosInseridos}");
          log.Info($"Finalizando cópia da tabela {tabela} =============================");
        }

      }
      catch (Exception ex)
      {
        log.Error($"Erro genérico na execução.", ex);

      }
    }
  }
}
