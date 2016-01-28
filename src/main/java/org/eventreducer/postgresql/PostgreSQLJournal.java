package org.eventreducer.postgresql;

import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.eventreducer.*;
import org.eventreducer.hlc.PhysicalTimeProvider;
import org.eventreducer.json.ObjectMapper;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class PostgreSQLJournal extends Journal {


    @Setter @Accessors(chain = true)
    private int fetchSize = 1024;
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
    public Optional<Event> findEvent(UUID uuid) {
        Connection conn = dataSource.getConnection();

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT event FROM journal WHERE uuid::text = ?");
        preparedStatement.setString(1, uuid.toString());

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            Event event = objectMapper.readValue(resultSet.getString("event"), Event.class);
            preparedStatement.close();
            conn.close();
            return Optional.of(event);
        }

        preparedStatement.close();
        conn.close();

        return Optional.empty();
    }

    @Override
    @SneakyThrows
    public Optional<Command> findCommand(UUID uuid) {
        Connection conn = dataSource.getConnection();

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT command FROM commands WHERE uuid::text = ?");
        preparedStatement.setString(1, uuid.toString());

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            Command command = objectMapper.readValue(resultSet.getString("command"), Command.class);
            preparedStatement.close();
            conn.close();
            return Optional.of(command);
        }

        preparedStatement.close();
        conn.close();

        return Optional.empty();
    }

    @Override
    @SneakyThrows
    protected long journal(Command command, Stream<Event> events) {
        Connection conn = dataSource.getConnection();

        conn.setAutoCommit(false);

        try {

            PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO commands (uuid, command) VALUES (?::UUID, ?::JSONB)");

            preparedStatement.setString(1, command.uuid().toString());
            preparedStatement.setString(2, objectMapper.writeValueAsString(command));

            preparedStatement.execute();
            preparedStatement.close();

            long count = events.map(new Function<Event, Event>() {
                @Override
                @SneakyThrows
                public Event apply(Event event) {

                    PreparedStatement stmt = conn.prepareStatement("INSERT INTO journal (uuid, event, command) VALUES (?::UUID, ?::JSONB, ?::UUID)");

                    stmt.setString(1, event.uuid().toString());
                    stmt.setString(2, objectMapper.writeValueAsString(event));
                    stmt.setString(3, command.uuid().toString());
                    stmt.addBatch();

                    stmt.execute();
                    stmt.close();

                    return event;
                }
            }).count();

            conn.commit();

            return count;
        } catch (Exception e) {
            conn.rollback();
            conn.close();
            throw e;
        } finally {
            conn.close();
        }
    }

    @Override
    @SneakyThrows
    public long size(Class<? extends Identifiable> klass) {
        Connection conn = dataSource.getConnection();

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT count(uuid) FROM journal WHERE event->>'@class' = ?");
        preparedStatement.setString(1, klass.getName());

        ResultSet resultSet = preparedStatement.executeQuery();

        resultSet.next();
        long result = resultSet.getLong(1);

        preparedStatement.close();

        preparedStatement = conn.prepareStatement("SELECT count(uuid) FROM commands  WHERE command->>'@class' = ?");
        preparedStatement.setString(1, klass.getName());

        resultSet = preparedStatement.executeQuery();
        resultSet.next();

        result += resultSet.getLong(1);

        preparedStatement.close();
        conn.close();

        return result;
    }


    @Override @SneakyThrows
    public Iterator<Event> eventIterator(Class<? extends Event> klass) {
        Connection conn = dataSource.getConnection();
        conn.setAutoCommit(false);

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT event FROM journal WHERE event->>'@class' = ?");
        preparedStatement.setString(1, klass.getName());

        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.setFetchSize(fetchSize);

        return new ObjectIterator<>(resultSet, conn, klass);
    }

    @Override @SneakyThrows
    public Iterator<Command> commandIterator(Class<? extends Command> klass) {
        Connection conn = dataSource.getConnection();
        conn.setAutoCommit(false);

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT command FROM commands WHERE command->>'@class' = ?");
        preparedStatement.setString(1, klass.getName());

        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.setFetchSize(fetchSize);

        return new ObjectIterator<>(resultSet, conn, klass);
    }

    @Override
    @SneakyThrows
    public Stream<Event> events(Command command) {
        Connection conn = dataSource.getConnection();
        conn.setAutoCommit(false);

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT event FROM journal WHERE command = ?::UUID");
        preparedStatement.setString(1, command.uuid().toString());

        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.setFetchSize(fetchSize);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ObjectIterator<>(resultSet, conn, Event.class), Spliterator.ORDERED), false);
    }


    class ObjectIterator<O> implements Iterator<O> {


        private final ResultSet resultSet;
        private final Connection conn;
        private final Class<? extends Identifiable> klass;

        public ObjectIterator(ResultSet resultSet, Connection conn, Class<? extends Identifiable> klass) {
            this.resultSet = resultSet;
            this.conn = conn;
            this.klass = klass;
        }

        @Override
        protected void finalize() throws Throwable {
            if (!resultSet.isClosed()) {
                resultSet.close();
            }

            if (!conn.isClosed()) {
                conn.close();
            }
        }

        @Override @SneakyThrows
        public boolean hasNext() {
            if (resultSet.isClosed()) {
                return false;
            }
            boolean next = resultSet.next();
            if (!next) {
                resultSet.close();
                conn.close();
            }
            return next;
        }

        @Override @SneakyThrows
        public O next() {
            O o = objectMapper.readValue(resultSet.getString(1), (Class<O>)klass);
            return o;
        }
    }


}
