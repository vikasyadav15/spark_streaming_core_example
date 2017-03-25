        import java.io.Serializable;
        import org.apache.spark.rdd.NewHadoopPartition;
        import org.apache.spark.rdd.UnionPartition;
        import org.apache.spark.Partition;
public class GetFileNameFromStream implements Serializable {


    public String getFileName(Partition partition) {
        UnionPartition upp = (UnionPartition) partition;
        NewHadoopPartition npp = (NewHadoopPartition) upp.parentPartition();
        String filePath = npp.serializableHadoopSplit().value().toString();
        return filePath;
    }

}