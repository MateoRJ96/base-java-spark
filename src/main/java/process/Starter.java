package process;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.*;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class Starter {
    public static void main(String[] args) throws URISyntaxException {
        System.setProperty("hadoop.home.dir", "D:\\git\\base-java-spark\\hadoop-3.2.1");

        URL res = Starter.class.getClassLoader().getResource("dxp-results-20210825-141347.csv");
        File archivoEntrada = Paths.get(res.toURI()).toFile();
        String pathFile = archivoEntrada.getAbsolutePath();; // Should be some file on your system
        SparkSession spark = SparkSession
                .builder()
                .config("spark.master", "local")
                .appName("Simple Application")
                .getOrCreate(); // Crear instancia de la sesion para spark

        // definicion de esquema
        StructType schema = new StructType(
                new StructField[]{
                        new StructField("cifras_tipo_m", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("sistema", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("linea_negocio", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("anio", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("mes", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("primas", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("recargos", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("derechos", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("iva", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("prima_total", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("sistema_ac", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("linea_negocio_ac", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("anio_ac", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("mes_ac", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("primas_ac", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("recargos_ac", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("derechos_ac", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("iva_ac", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("prima_total_ac", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("sistema_diff", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("linea_negocio_diff", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("anio_diff", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("mes_diff", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("primas_diff", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("recargos_diff", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("derechos_diff", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("iva_diff", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("prima_total_diff", DataTypes.DoubleType, true, Metadata.empty())
                }
        );
        // cargar dataframe
        // se coloca la opcion header a true para que no salga como parte de los datos
        Dataset<Row> datos = spark.read().schema(schema).option("header", "true").csv(pathFile);
        // mostrar datos
        datos.show();
        // select de columnas
        Dataset<Row> seleccion = datos.select("sistema", "anio", "mes", "primas").distinct();
        seleccion.show();
        // filtro en columna primas donde los montos sean menores a 0
        // y solo mostrar la columna sistema, año , mes y primas
        Dataset<Row> filtro = datos
                .filter(datos.col("primas").$less(0))
                .select("sistema", "anio", "mes", "primas");
        filtro.show();
    }
}
