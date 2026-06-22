package com.etl.connector.doris.sink.config;

import com.etl.core.config.SinkConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

class DorisSinkConfigTest {

    private SinkConfig sinkConfig(Map<String, Object> props) {
        SinkConfig config = new SinkConfig();
        config.setType("doris");
        config.setConfig(props);
        return config;
    }

    private Map<String, Object> validProps() {
        Map<String, Object> p = new HashMap<>();
        p.put("fenodes", "127.0.0.1:8030");
        p.put("tableIdentifier", "test_db.test_tbl");
        p.put("username", "root");
        p.put("password", "");
        return p;
    }

    @Test
    void fromSinkConfig_missingFenodes_throws() {
        Map<String, Object> p = validProps();
        p.remove("fenodes");
        assertThrows(IllegalArgumentException.class, () -> DorisSinkConfig.fromSinkConfig(sinkConfig(p)));
    }

    @Test
    void fromSinkConfig_missingTableIdentifier_throws() {
        Map<String, Object> p = validProps();
        p.remove("tableIdentifier");
        assertThrows(IllegalArgumentException.class, () -> DorisSinkConfig.fromSinkConfig(sinkConfig(p)));
    }

    @Test
    void fromSinkConfig_tableIdentifierWithoutDot_throws() {
        Map<String, Object> p = validProps();
        p.put("tableIdentifier", "no_dot_table");
        assertThrows(IllegalArgumentException.class, () -> DorisSinkConfig.fromSinkConfig(sinkConfig(p)));
    }
}
