package org.apache.spark.sql;

import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestMergeIntoSQLCommand {

    @Test
    public void testMergeIntoSQLBasicNotMatch() throws IOException {
        String SQLText = "MERGE INTO LOGS\n" +
                "USING NEWDEDUPEDLOGS\n" +
                "ON LOGS.UNIQUEID = NEWDEDUPEDLOGS.UNIQUEID\n" +
                "WHEN NOT MATCHED THEN INSERT *;";
    }

    public void testMergeIntoSQLBasicMatch() throws IOException {
        String SQLText = "MERGE INTO LOGS\n" +
                "USING NEWDEDUPEDLOGS\n" +
                "ON LOGS.UNIQUEID = NEWDEDUPEDLOGS.UNIQUEID\n" +
                "WHEN MATCHED THEN DELETE;";
    }


    public void testMergeIntoSQLBasic() throws IOException {
        String SQLText = "MERGE INTO TARGET\n" +
                "USING SOURCE\n" +
                "ON TARGET.UNIQUEID = SOURCE.UNIQUEID\n" +
                "WHEN MATCHED AND (TARGET.COL2='update') THEN UPDATE SET TARGET.PRICE=SOURCE.PRICE\n" +
                "WHEN MATCHED AND (TARGET.COL2='delete') THEN DELETE\n" +
                "WHEN MATCHED THEN DELETE\n" +
                "WHEN NOT MATCHED THEN INSERT *\n" +
                "WHEN NOT MATCHED AND (TARGET.COL2='insert') THEN INSERT *;";
    }

    public void testMergeIntoSQLComplex() throws IOException {
        String SQLText = "MERGE INTO TARGET\n" +
                "USING SOURCE\n" +
                "ON TARGET.UNIQUEID = SOURCE.UNIQUEID\n" +
                "WHEN MATCHED AND (TARGET.COL2='update') THEN UPDATE SET TARGET.PRICE=SOURCE.PRICE\n" +
                "WHEN MATCHED AND (TARGET.COL2='delete') THEN DELETE\n" +
                "WHEN MATCHED THEN DELETE\n" +
                "WHEN MATCHED THEN UPDATE SET *\n" +
                "WHEN NOT MATCHED THEN INSERT *\n" +
                "WHEN NOT MATCHED AND (TARGET.COL2='insert') THEN INSERT *\n" +
                "WHEN NOT MATCHED AND (TARGET.COL2='insert') THEN INSERT (TARGET.COL1, TARGET.COL2) VALUES (SOURCE.COL1, SOURCE.COL2);";
    }
}
