package migrations;

import lombok.extern.slf4j.Slf4j;
import org.eventreducer.Serializable;
import org.eventreducer.json.ObjectMapper;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class V5_3__Convert_Events_To_Payloads implements JdbcMigration {
    @Override
    public void migrate(Connection connection) throws Exception {
        ResultSet count = connection.prepareStatement("SELECT count(uuid) FROM journal").executeQuery();
        count.next();
        long cnt = count.getLong(1);
        long pct = cnt / 100;

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT uuid::text, event, event->>'@class' FROM journal");
        ResultSet resultSet = preparedStatement.executeQuery();
        ObjectMapper objectMapper = new ObjectMapper();

        long l = 0;
        long completed = 0;
        log.info("Converting {} events", cnt);

        Map<String, Class<? extends Serializable>> klasses = new HashMap<>();

        PreparedStatement update = connection.prepareStatement("UPDATE journal SET payload = ? WHERE uuid = ?::uuid");

        while (resultSet.next()) {
            l++;

            String className = resultSet.getString(3);
            Class<? extends Serializable> aClass = klasses.get(className);

            if (aClass == null) {
                aClass = (Class<? extends Serializable>) Class.forName(className);
                klasses.put(className, aClass);
            }

            Serializable o = objectMapper.readValue(resultSet.getCharacterStream(2), aClass);
            ByteBuffer buffer = ByteBuffer.allocate(o.entitySerializer().size(o));
            o.entitySerializer().serialize(o, buffer);
            byte[] bytes = Arrays.copyOfRange(buffer.array(), 0, buffer.position());

            update.setBytes(1, bytes);
            update.setString(2, resultSet.getString(1));
            update.addBatch();

            if (l % pct == 0) {
                completed++;
                log.info("Processed {}%", completed);
                update.executeBatch();
            }
        }

        resultSet.close();

    }
}
