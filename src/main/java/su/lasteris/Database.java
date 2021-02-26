package su.lasteris;

import io.agroal.api.AgroalDataSource;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

@Singleton
public class Database {
    @Inject
    AgroalDataSource agroalDataSource;

    public <T> T execute(String sql, List<Object> params, Function<ResultSet, T> mapper) {
        try(Connection c = agroalDataSource.getConnection()) {
            return operation(c, sql, params, mapper);
        } catch (SQLException e) {
            throw new RuntimeException("ошибка при выполнении получении запроса к db", e);
        }
    }

    public long copyIn(String sql, BufferedReader reader) {
        long rowInserted;
        try(Connection c = agroalDataSource.getConnection()) {
            CopyManager cm = new CopyManager(c.unwrap(BaseConnection.class));
           rowInserted = cm.copyIn(sql, reader);
        } catch (SQLException | IOException e) {
            throw new RuntimeException("Внутренняя ошибка сервера", e);
        }
        return rowInserted;
    }
    public long executeInsert(String sql, List<Object> params) {
        try (Connection c = agroalDataSource.getConnection()) {
            return prepareInsert(c, sql, params);
        } catch (SQLException e) {
            throw new RuntimeException("Внутренняя ошибка сервера", e);
        }
    }

    public long executeUpdate(String sql, List<Object> params) {
        try (Connection c = agroalDataSource.getConnection()) {
            return prepareUpdate(c, sql, params);
        } catch (SQLException e) {
            throw new RuntimeException("Внутренняя ошибка сервера", e);
        }
    }

    private long prepareInsert(Connection c, String sql, List<Object> params) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            prepareStatement(ps, params);
            return fetchInsert(ps);
        }
    }

    private long prepareUpdate(Connection c, String sql, List<Object> params) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            prepareStatement(ps, params);
            return fetchUpdate(ps);
        }
    }

    private long fetchInsert(PreparedStatement ps) throws SQLException {
        ps.executeUpdate();

        try (ResultSet rs = ps.getGeneratedKeys()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private int fetchUpdate(PreparedStatement ps) throws SQLException {
        return ps.executeUpdate();
    }

    public <T> T operation(Connection c, String sql, List<Object> params, Function<ResultSet, T> mapper) {
        try(PreparedStatement ps = c.prepareStatement(sql)) {
            prepareStatement(ps, params);
            return fetchData(ps, mapper);
        } catch (SQLException e) {
            throw new RuntimeException("ошибка при формировании", e);
        }
    }

    private void prepareStatement(PreparedStatement ps, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }
    }

    private <T> T fetchData(PreparedStatement ps, Function<ResultSet, T> mapper) {
        try(ResultSet rs = ps.executeQuery()) {
            return mapper ==null ? null : mapper.apply(rs);
        } catch (SQLException e) {
            throw new RuntimeException("ошибка при получении данных из db", e);
        }
    }


}
