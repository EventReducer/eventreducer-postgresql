package org.eventreducer.postgresql;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eventreducer.Command;
import org.eventreducer.Event;
import org.eventreducer.IndexFactory;
import org.eventreducer.Journal;
import org.eventreducer.hlc.PhysicalTimeProvider;
import org.eventreducer.json.ObjectMapper;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

@Slf4j
public class PostgreSQLJournal extends Journal {


    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    @SneakyThrows
    public PostgreSQLJournal(PhysicalTimeProvider physicalTimeProvider, DataSource dataSource) {
        super(physicalTimeProvider);

        checkVersion(dataSource);

        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        flyway.setLocations("migrations");
        flyway.migrate();
        this.dataSource = dataSource;
        this.objectMapper = new ObjectMapper();
    }

    private void checkVersion(DataSource dataSource) throws Exception {
        Connection conn = dataSource.getConnection();

        PreparedStatement preparedStatement = conn.prepareStatement("SHOW server_version");

        ResultSet resultSet = preparedStatement.executeQuery();

        resultSet.next();

        if (new Version(resultSet.getString(1)).compareTo(new Version("9.5.0")) < 0) {
            throw new Exception("PostgreSQL " + resultSet.getString(1) + " is too old, 9.5 is required");
        }
        preparedStatement.close();
        conn.close();
    }


    @Override
    @SneakyThrows
    public void prepareIndices(IndexFactory indexFactory) {
        super.prepareIndices(indexFactory);

        Connection conn = dataSource.getConnection();

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT event FROM journal");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            Event event = objectMapper.readValue(resultSet.getString("event"), Event.class);
            try {
                event.entitySerializer().index(indexFactory, event);
            } catch (Exception e) {
                log.error("Error while processing event {}: {}", event.uuid(), e.getMessage());
            }
        }

        preparedStatement.close();
        conn.close();

    }

    @Override
    @SneakyThrows
    protected void journal(Command command, List<Event> events) {
        Connection conn = dataSource.getConnection();

        conn.setAutoCommit(false);

        try {

            PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO commands (uuid, command) VALUES (?::UUID, ?::JSONB)");

            preparedStatement.setString(1, command.uuid().toString());
            preparedStatement.setString(2, objectMapper.writeValueAsString(command));

            preparedStatement.execute();
            preparedStatement.close();

            preparedStatement = conn.prepareStatement("INSERT INTO journal (uuid, event, command) VALUES (?::UUID, ?::JSONB, ?::UUID)");

            for (Event event : events) {
                preparedStatement.setString(1, event.uuid().toString());
                preparedStatement.setString(2, objectMapper.writeValueAsString(event));
                preparedStatement.setString(3, command.uuid().toString());
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            preparedStatement.close();

            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            conn.close();
            throw e;
        }

        conn.close();
    }

    @Override
    @SneakyThrows
    public long size() {
        Connection conn = dataSource.getConnection();

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT count(uuid) FROM journal");

        ResultSet resultSet = preparedStatement.executeQuery();

        long result = resultSet.getLong(1);

        preparedStatement.close();
        conn.close();

        return result;
    }

}
