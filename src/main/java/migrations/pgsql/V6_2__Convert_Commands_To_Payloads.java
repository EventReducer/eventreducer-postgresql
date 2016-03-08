package migrations.pgsql;

import lombok.extern.slf4j.Slf4j;
import org.eventreducer.Command;
import org.eventreducer.Event;
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
public class V6_2__Convert_Commands_To_Payloads implements JdbcMigration {
    @Override
    public void migrate(Connection connection) throws Exception {
        ResultSet count = connection.prepareStatement("SELECT count(uuid) FROM commands").executeQuery();
        count.next();
        long cnt = count.getLong(1);
        long pct = cnt / 100;

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT uuid::text, command, command->>'@class' FROM commands");
        ResultSet resultSet = preparedStatement.executeQuery();
        ObjectMapper objectMapper = new ObjectMapper();

        long l = 0;
        long completed = 0;
        log.info("Converting {} commands", cnt);

        Map<String, Class<? extends Serializable>> klasses = new HashMap<>();

        PreparedStatement update = connection.prepareStatement("UPDATE commands SET payload = ?, hash = ?, created_at = ?, trace = ?::JSONB WHERE uuid = ?::uuid");
        com.fasterxml.jackson.databind.ObjectMapper traceMapper = new com.fasterxml.jackson.databind.ObjectMapper();

        while (resultSet.next()) {
            l++;

            String className = resultSet.getString(3);
            Class<? extends Serializable> aClass = klasses.get(className);

            if (aClass == null) {
                aClass = (Class<? extends Serializable>) Class.forName(className);
                klasses.put(className, aClass);
            }

            Serializable o = objectMapper.readValue(resultSet.getString(2), aClass);
            ByteBuffer buffer = ByteBuffer.allocate(o.entitySerializer().size(o));
            o.entitySerializer().serialize(o, buffer);
            byte[] bytes = Arrays.copyOfRange(buffer.array(), 0, buffer.position());

            update.setBytes(1, bytes);
            update.setBytes(2, o.entitySerializer().hash());
            update.setLong(3, ((Command)o).timestamp().ntpValue());
            update.setString(4, traceMapper.writeValueAsString(((Command)o).trace()));
            update.setString(5, resultSet.getString(1));
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
