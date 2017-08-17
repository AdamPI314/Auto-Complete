import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable {
    private  String phrase_1;
    private  String phrase_2;
    private int count;

    public DBOutputWritable(String phrase_1, String phrase_2, int count) {
        this.phrase_1 = phrase_1;
        this.phrase_2 = phrase_2;
        this.count = count;
    }

    public void readFields(ResultSet resultSet) throws SQLException {
       this.phrase_1 = resultSet.getString(1);
       this.phrase_2 = resultSet.getString(2);
       this.count = resultSet.getInt(3);
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
       preparedStatement.setString(1, phrase_1);
       preparedStatement.setString(2, phrase_2);
       preparedStatement.setInt(3, count);
    }
}
