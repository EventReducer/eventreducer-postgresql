package org.eventreducer.postgresql;

import com.google.common.io.BaseEncoding;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ntp.TimeStamp;
import org.eventreducer.*;
import org.eventreducer.hlc.PhysicalTimeProvider;
import org.eventreducer.json.ObjectMapper;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class PostgreSQLJournal extends Journal {


    @Accessors(chain = true)
    private final DataSource dataSource;

    private Map<String, Serializer<Serializable>> classMap = new HashMap<>();

    private AtomicBoolean dirty = new AtomicBoolean(false);
    private com.fasterxml.jackson.databind.ObjectMapper traceMapper;

    @SneakyThrows
    public PostgreSQLJournal(PhysicalTimeProvider physicalTimeProvider, DataSource dataSource) {
        super(physicalTimeProvider);

        checkVersion(dataSource);

        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        flyway.setLocations("migrations");
        flyway.migrate();
        this.dataSource = dataSource;
        traceMapper = new com.fasterxml.jackson.databind.ObjectMapper();
    }

    @Override
    @SneakyThrows
    public Journal endpoint(Endpoint endpoint) {
        super.endpoint(endpoint);
        endpoint.getSerializables().forEach(new Consumer<Class<? extends Serializable>>() {
            @Override
            @SneakyThrows
            public void accept(Class<? extends Serializable> aClass) {
                Serializer<Serializable> entitySerializer = aClass.newInstance().entitySerializer();
                String encodedHash = BaseEncoding.base16().encode(entitySerializer.hash());
                classMap.put(encodedHash, entitySerializer);
            }
        });
        return this;
    }

    private void checkVersion(DataSource dataSource) throws Exception {
        Connection conn = dataSource.getConnection();

        PreparedStatement preparedStatement = conn.prepareStatement("SHOW server_version");

        ResultSet resultSet = preparedStatement.executeQuery();

        resultSet.next();

        if (new Version(resultSet.getString(1)).compareTo(new Version("9.5.0")) < 0) {
            throw new Exception("PostgreSQL " + resultSet.getString(1) + " is too old, 9.5 is required");
        }

        resultSet.close();
        preparedStatement.close();
        conn.close();
    }

    @Override
    @SneakyThrows
    public Optional<Event> findEvent(UUID uuid) {
        Connection conn = dataSource.getConnection();

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT hash, payload FROM journal WHERE uuid = ?::uuid");
        preparedStatement.setString(1, uuid.toString());

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            Serializer<Serializable> entitySerializer = classMap.get(BaseEncoding.base16().encode(resultSet.getBytes(1)));
            byte[] payload = resultSet.getBytes(2);
            ByteBuffer buf = ByteBuffer.allocate(payload.length);
            buf.put(payload);
            buf.rewind();
            Event event = (Event) entitySerializer.deserialize(buf);
            resultSet.close();
            preparedStatement.close();
            conn.close();
            return Optional.of(event);
        }

        resultSet.close();
        preparedStatement.close();
        conn.close();

        return Optional.empty();
    }

    @Override
    @SneakyThrows
    public Optional<Command> findCommand(UUID uuid) {
        Connection conn = dataSource.getConnection();

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT hash, payload FROM commands WHERE uuid = ?::uuid");
        preparedStatement.setString(1, uuid.toString());

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            Serializer<Serializable> entitySerializer = classMap.get(BaseEncoding.base16().encode(resultSet.getBytes(1)));
            byte[] payload = resultSet.getBytes(2);
            ByteBuffer buf = ByteBuffer.allocate(payload.length);
            buf.put(payload);
            buf.rewind();
            Command cmd = (Command) entitySerializer.deserialize(buf);
            resultSet.close();
            preparedStatement.close();
            conn.close();
            return Optional.of(cmd);
        }

        resultSet.close();
        preparedStatement.close();
        conn.close();

        return Optional.empty();
    }

    @Override
    @SneakyThrows
    protected long journal(Command command, Stream<Event> events) {
        Connection conn = dataSource.getConnection();

        conn.setAutoCommit(false);
        conn.setReadOnly(false);

        try {

            String commandUUID = command.uuid().toString();

            PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO commands (uuid, hash, payload, created_at, trace) VALUES (?::UUID, ?, ?, ?, ?::JSONB)");

            ByteBuffer buffer = ByteBuffer.allocate(command.entitySerializer().size(command));
            preparedStatement.setString(1, commandUUID);
            preparedStatement.setBytes(2, command.entitySerializer().hash());
            preparedStatement.setBytes(3, buffer.array());
            preparedStatement.setLong(4, command.timestamp().ntpValue());
            preparedStatement.setString(5, traceMapper.writeValueAsString(command.trace()));

            preparedStatement.executeUpdate();
            preparedStatement.close();

            PreparedStatement stmt = conn.prepareStatement("INSERT INTO journal (uuid, hash, payload, command, created_at) VALUES (?::UUID, ?, ?, ?::UUID, ?)");

            long count = events.peek(new Consumer<Event>() {
                @Override
                @SneakyThrows
                public void accept(Event event) {
                    ByteBuffer buffer = ByteBuffer.allocate(event.entitySerializer().size(event));
                    event.entitySerializer().serialize(event, buffer);
                    int position = buffer.position();
                    buffer.rewind();
                    byte[] payload = Arrays.copyOfRange(buffer.array(), 0, position);

                    stmt.setString(1, event.uuid().toString());
                    stmt.setBytes(2, event.entitySerializer().hash());
                    stmt.setBytes(3, payload);
                    stmt.setString(4, commandUUID);
                    stmt.setLong(5, event.timestamp().ntpValue());

                    stmt.executeUpdate();
                }
            }).count();

            stmt.close();

            conn.commit();

            eventIterators.clear();
            commandIterators.clear();
            dirty.set(true);

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
    public long size(Class<? extends Serializable> klass) {
        Connection conn = dataSource.getConnection();
        conn.setReadOnly(true);
        conn.setAutoCommit(false);

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT count(uuid) FROM journal WHERE hash = ?");
        preparedStatement.setBytes(1, klass.newInstance().entitySerializer().hash());

        ResultSet resultSet = preparedStatement.executeQuery();

        resultSet.next();
        long result = resultSet.getLong(1);

        preparedStatement.close();

        preparedStatement = conn.prepareStatement("SELECT count(uuid) FROM commands WHERE hash = ?");
        preparedStatement.setBytes(1, klass.newInstance().entitySerializer().hash());

        resultSet = preparedStatement.executeQuery();
        resultSet.next();

        result += resultSet.getLong(1);

        resultSet.close();
        preparedStatement.close();
        conn.rollback();
        conn.close();

        return result;
    }

    @Override
    @SneakyThrows
    public boolean isEmpty(Class<? extends Serializable> klass) {
        Connection conn = dataSource.getConnection();
        conn.setReadOnly(true);
        conn.setAutoCommit(false);

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT uuid FROM journal WHERE hash = ? LIMIT 1");
        preparedStatement.setBytes(1, klass.newInstance().entitySerializer().hash());

        ResultSet resultSetEvents = preparedStatement.executeQuery();

        preparedStatement.clearParameters();
        preparedStatement = conn.prepareStatement("SELECT uuid FROM commands WHERE hash = ? LIMIT 1");
        preparedStatement.setBytes(1, klass.newInstance().entitySerializer().hash());

        ResultSet resultSetCommands = preparedStatement.executeQuery();

        boolean empty = !resultSetEvents.next() && !resultSetCommands.next();

        resultSetEvents.close();
        resultSetCommands.close();
        preparedStatement.close();
        conn.rollback();
        conn.close();

        return empty;
    }

    private Map<String, Iterator<Event>> eventIterators = new ConcurrentHashMap<>();

    @Override @SneakyThrows
    public Iterator<Event> eventIterator(Class<? extends Event> klass) {
        byte[] hash = klass.newInstance().entitySerializer().hash();
        String encodedHash = BaseEncoding.base16().encode(hash);
        if (!dirty.get() && eventIterators.containsKey(encodedHash)) {
            return eventIterators.get(encodedHash);
        } else {
            Connection conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            conn.setReadOnly(true);

            PreparedStatement preparedStatement = conn.prepareStatement("SELECT uuid, hash, payload, created_at FROM journal WHERE hash = ?");
            preparedStatement.setBytes(1, hash);

            ResultSet resultSet = preparedStatement.executeQuery();

            dirty.set(false);

            CachingIterator<Event> eventCachingIterator = new CachingIterator<>(new BinaryObjectIterator<>(resultSet, preparedStatement, conn, classMap));
            eventIterators.put(encodedHash, eventCachingIterator);
            return eventCachingIterator;
        }
    }

    private Map<String, Iterator<Command>> commandIterators = new ConcurrentHashMap<>();

    @Override @SneakyThrows
    public Iterator<Command> commandIterator(Class<? extends Command> klass) {
        byte[] hash = klass.newInstance().entitySerializer().hash();
        String encodedHash = BaseEncoding.base16().encode(hash);
        if (!dirty.get() && commandIterators.containsKey(encodedHash)) {
            return commandIterators.get(encodedHash);
        } else {
            Connection conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            conn.setReadOnly(true);

            PreparedStatement preparedStatement = conn.prepareStatement("SELECT uuid, hash, payload, created_at, trace FROM commands WHERE hash = ?");
            preparedStatement.setBytes(1, hash);

            ResultSet resultSet = preparedStatement.executeQuery();

            dirty.set(false);

            CachingIterator<Command> commandCachingIterator = new CachingIterator<>(new ObjectIterator<>(resultSet, preparedStatement, conn, klass));
            commandIterators.put(encodedHash, commandCachingIterator);
            return commandCachingIterator;
        }
    }

    @Override
    @SneakyThrows
    public Stream<Event> events(Command command) {
        Connection conn = dataSource.getConnection();
        conn.setAutoCommit(false);
        conn.setReadOnly(true);

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT uuid, hash, payload, created_at FROM journal WHERE command = ?::UUID");
        preparedStatement.setString(1, command.uuid().toString());

        ResultSet resultSet = preparedStatement.executeQuery();

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new BinaryObjectIterator<>(resultSet, preparedStatement, conn, classMap), Spliterator.ORDERED), false);
    }


    static class ObjectIterator<O extends Serializable> implements Iterator<O> {


        private final ResultSet resultSet;
        private final PreparedStatement preparedStatement;
        private final Connection conn;
        private final Class<? extends Identifiable> klass;
        private final ObjectMapper objectMapper;

        public ObjectIterator(ResultSet resultSet, PreparedStatement preparedStatement, Connection conn, Class<? extends Identifiable> klass) {
            this.resultSet = resultSet;
            this.preparedStatement = preparedStatement;
            this.conn = conn;
            this.klass = klass;
            this.objectMapper = new ObjectMapper();
        }

        @Override
        protected void finalize() throws Throwable {
            if (!resultSet.isClosed()) {
                resultSet.close();
            }

            if (!preparedStatement.isClosed()) {
                preparedStatement.close();
            }

            if (!conn.isClosed()) {
                conn.rollback();
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
                preparedStatement.close();
                resultSet.close();
                conn.rollback();
                conn.close();
            }
            return next;
        }

        @Override @SneakyThrows
        public O next() {
            O o = objectMapper.readValue(resultSet.getCharacterStream(1), (Class<O>)klass);
            return o;
        }
    }


    static class BinaryObjectIterator<O extends Serializable> implements Iterator<O> {


        private final ResultSet resultSet;
        private final PreparedStatement preparedStatement;
        private final Connection conn;
        private Map<String, Serializer<Serializable>> classMap;

        public BinaryObjectIterator(ResultSet resultSet, PreparedStatement preparedStatement, Connection conn, Map<String, Serializer<Serializable>> classMap) {
            this.resultSet = resultSet;
            this.preparedStatement = preparedStatement;
            this.conn = conn;
            this.classMap = classMap;
        }

        @Override
        protected void finalize() throws Throwable {
            if (!resultSet.isClosed()) {
                resultSet.close();
            }

            if (!preparedStatement.isClosed()) {
                preparedStatement.close();
            }

            if (!conn.isClosed()) {
                conn.rollback();
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
                preparedStatement.close();
                resultSet.close();
                conn.rollback();
                conn.close();
            }
            return next;
        }

        @Override @SneakyThrows
        public O next() {
            UUID uuid = UUID.fromString(resultSet.getString(1));
            String encodedHash = BaseEncoding.base16().encode(resultSet.getBytes(2));
            Serializer<Serializable> entitySerializer = classMap.get(encodedHash);
            O o = (O) entitySerializer.deserialize(ByteBuffer.wrap(resultSet.getBytes(3)));
            if (o instanceof Event) {
                ((Event)o).uuid(uuid);
                ((Event)o).timestamp(new TimeStamp(resultSet.getLong(4)));
            }
            if (o instanceof Command) {
                ((Command)o).uuid(uuid);
                ((Command)o).timestamp(new TimeStamp(resultSet.getLong(4)));
                if (resultSet.getString(5) != null) {
                    ((Command)o).trace = resultSet.getString(5);
                }
            }
            return o;
        }
    }

    static class CachingIterator<O> implements Iterator<O> {

        private Iterator<O> backingIterator;
        private List<O> cache = new ArrayList<>();
        private boolean usingCache = false;

        public CachingIterator(Iterator<O> backingIterator) {
            this.backingIterator = backingIterator;
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = backingIterator.hasNext();
            if (!hasNext) {
                backingIterator = cache.iterator();
                usingCache = true;
            }
            return hasNext;
        }

        @Override
        public O next() {
            O next = backingIterator.next();
            if (!usingCache) {
                cache.add(next);
            }
            return next;
        }
    }


}
