using Apache.Arrow;
using Apache.Arrow.Types;
using System.ComponentModel.DataAnnotations;

namespace MyTest
{
    public class Program
    {
        public static void Main()
        {
            var builder = new DoubleArray.Builder();
            builder.AppendNull();
            builder.Append(1);
            builder.Append(2);
            builder.Append(3);
            var array = builder.Build();
            Console.WriteLine(array);
            var builder2 = new StringArray.Builder();
            builder2.AppendNull();
            builder2.Append("a");
            Console.WriteLine(builder2.Build());
            var builder3 = new BooleanArray.Builder();
            builder3.AppendNull();
            builder3.Append(true);
            Console.WriteLine(builder3.Build());
        }
    }
}