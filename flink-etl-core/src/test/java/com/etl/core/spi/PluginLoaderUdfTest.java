package com.etl.core.spi;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PluginLoaderUdfTest {

    @Test
    void testLoadAllUdfPlugins() {
        List<UdfPlugin> plugins = PluginLoader.loadAllUdfPlugins();

        // 验证加载成功
        assertNotNull(plugins);

        // 验证至少包含 HashUdf
        assertTrue(plugins.stream().anyMatch(p -> "hash_code".equals(p.identifier())));
    }

    @Test
    void testLoadAllUdfPluginsNotEmpty() {
        List<UdfPlugin> plugins = PluginLoader.loadAllUdfPlugins();

        // 验证列表不为空
        assertFalse(plugins.isEmpty());

        // 验证每个插件的 identifier 非空
        for (UdfPlugin plugin : plugins) {
            assertNotNull(plugin.identifier());
            assertFalse(plugin.identifier().trim().isEmpty());
        }
    }
}