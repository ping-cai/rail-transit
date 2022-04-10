import java.io.IOException;
import java.sql.SQLException;

public class GenerateAfcTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException, IOException {
        GenerateAfc generateAfc = new GenerateAfc();
        generateAfc.generate(100);
    }
}
