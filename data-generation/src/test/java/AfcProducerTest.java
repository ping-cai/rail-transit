import afc.AfcRecord;

import java.io.IOException;

public class AfcProducerTest {
    public static void main(String[] args) throws IOException {
        AfcProducer afcProducer = new AfcProducer();
        AfcRecord oneRecord = AfcRecord.getOneRecord();
        afcProducer.sendAfc(oneRecord);
    }
}
