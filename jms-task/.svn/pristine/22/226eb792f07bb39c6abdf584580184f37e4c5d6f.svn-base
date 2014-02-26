package com.bleum.canton.jms.assist;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.bleum.canton.common.util.CantonStringUtils;
import com.bleum.canton.jms.entity.JMSTask;

/**
 * 
 * Description: Helper class for unit test.
 * 
 * @author jay.li
 * @created Feb 25, 2014 3:46:23 PM
 * 
 */
@Component("jmsTestUtils")
public class JMSTestUtils {
    private static final String QUERY_ALL_TASK = "select * from jms_b_task";

    private static final String QUERY_TASK_TYPEANDSTATUS = "select * from jms_b_task where type = ? and status = ?";

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public JMSTask getTaskById(long id) {
        JMSTask result = this.jdbcTemplate.queryForObject(
                "select * from jms_b_task where id = ?", new Object[] { id },
                new RowMapper<JMSTask>() {
                    public JMSTask mapRow(ResultSet rs, int rowNum)
                            throws SQLException {
                        JMSTask task = new JMSTask();
                        task.setId(rs.getLong("id"));
                        task.setContentId(rs.getString("content_id"));
                        task.setType(rs.getInt("type"));
                        task.setStatus(rs.getInt("status"));
                        task.setCreatedDate(rs.getDate("created_time"));
                        task.setOperatedDate(rs.getDate("operated_time"));
                        task.setMessage(CantonStringUtils.convertToString(rs
                                .getClob("message")));
                        task.setRetry(rs.getInt("retry"));
                        task.setLastError(rs.getString("last_error"));
                        return task;
                    }
                });
        return result;
    }

    public int lastId() {
        return this.jdbcTemplate.queryForInt("select LAST_INSERT_ID()");

    }

    public List<JMSTask> findAllTask() {
        return this.jdbcTemplate.query(QUERY_ALL_TASK, new Object[] {},
                new RowMapper<JMSTask>() {
                    public JMSTask mapRow(ResultSet rs, int rowNum)
                            throws SQLException {
                        JMSTask task = new JMSTask();
                        task.setId(rs.getLong("id"));
                        task.setContentId(rs.getString("content_id"));
                        task.setType(rs.getInt("type"));
                        task.setStatus(rs.getInt("status"));
                        task.setCreatedDate(rs.getDate("created_time"));
                        task.setOperatedDate(rs.getDate("operated_time"));
                        task.setMessage(CantonStringUtils.convertToString(rs
                                .getClob("message")));
                        task.setRetry(rs.getInt("retry"));
                        task.setLastError(rs.getString("last_error"));
                        return task;
                    }
                });
    }

    public List<JMSTask> findTaskByTypeAndStatus(int type, int status) {
        return this.jdbcTemplate.query(QUERY_TASK_TYPEANDSTATUS, new Object[] {
                type, status }, new RowMapper<JMSTask>() {
            public JMSTask mapRow(ResultSet rs, int rowNum) throws SQLException {
                JMSTask task = new JMSTask();
                task.setId(rs.getLong("id"));
                task.setContentId(rs.getString("content_id"));
                task.setType(rs.getInt("type"));
                task.setStatus(rs.getInt("status"));
                task.setCreatedDate(rs.getDate("created_time"));
                task.setOperatedDate(rs.getDate("operated_time"));
                task.setMessage(CantonStringUtils.convertToString(rs
                        .getClob("message")));
                task.setRetry(rs.getInt("retry"));
                task.setLastError(rs.getString("last_error"));
                return task;
            }
        });
    }

}
