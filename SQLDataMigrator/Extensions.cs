using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataMigrator.Extensions
{
  public static class Extensions
  {
    public static object RecuperarValorColuna(this IDictionary<string, object> @object, string key)
    {
      if (!@object.ContainsKey(key))
        throw new ApplicationException($"A coluna '{key}' não existe no registro da tabela");

      var keyValue = @object[key];

      if (keyValue is bool)
        return (bool)keyValue ? 1 : 0;

      return keyValue;
    }
  }
}
