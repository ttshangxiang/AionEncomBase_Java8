/**
 * This file is part of the Aion Reconstruction Project Server.
 *
 * The Aion Reconstruction Project Server is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * The Aion Reconstruction Project Server is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with the Aion Reconstruction Project Server. If not see
 * <http://www.gnu.org/licenses/>.
 *
 * @AionReconstructionProjectTeam
 */
package com.aionemu.gameserver.world.geo.nav;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aionemu.gameserver.configs.main.GeoDataConfig;
import com.aionemu.gameserver.dataholders.DataManager;
import com.aionemu.gameserver.geoEngine.models.GeoMap;
import com.aionemu.gameserver.geoEngine.scene.NavGeometry;
import com.aionemu.gameserver.model.templates.world.WorldMapTemplate;

/**
 * Thread-safe lazy loader for navigation mesh data.
 * Loads nav meshes on-demand and caches them for future use.
 * Memory-efficient: only loads maps that are actually accessed.
 * 
 * @author Yon (Aion Reconstruction Project)
 */
public class NavData {

    private static final Logger LOG = LoggerFactory.getLogger(NavData.class);
    
    // ================ CONSTANTS ================
    
    /** Navigation data directory */
    private static final String NAV_DIR = "./data/nav/";
    
    /** Size of float in bytes */
    private static final int FLOAT_SIZE_BYTES = 4;
    
    /** Number of components per vertex (x, y, z) */
    private static final int VERTEX_COMPONENTS = 3;
    
    /** Size of one vertex in bytes */
    private static final int VERTEX_STRIDE_BYTES = FLOAT_SIZE_BYTES * VERTEX_COMPONENTS;
    
    /** Header size: one int for float count (legacy format) */
    private static final int HEADER_SIZE_BYTES = FLOAT_SIZE_BYTES;
    
    /** Legacy format: total float count (not vertex count) */
    private static final int LEGACY_HEADER_SIZE_BYTES = FLOAT_SIZE_BYTES;
    
    // 【修正】2026-03-07 - 恢复旧版文件格式解析逻辑
    // 旧版 .nav 文件格式：文件头存储的是总浮点数数量（floatCount），而不是顶点数量（vertexCount）
    // 修复原因：新版代码使用了错误的文件格式解析方式，导致 nav 文件加载失败
    // 修复方案：恢复旧版的文件格式解析逻辑，兼容旧版 nav 文件
    
    // ================ CACHES ================
    
    /**
     * Thread-safe cache of loaded navigation maps.
     * Uses ConcurrentHashMap for lock-free reads and atomic lazy loading.
     */
    private final ConcurrentHashMap<Integer, GeoMap> navMaps = new ConcurrentHashMap<>();
    
    /**
     * File index - stores only file references, not the actual data.
     * Memory-efficient, populated at startup.
     */
    private final ConcurrentHashMap<Integer, File> navFiles = new ConcurrentHashMap<>();
    
    /**
     * Optional: SoftReference-based cache for memory-sensitive environments.
     * Can be enabled via config.
     */
    private final ConcurrentHashMap<Integer, java.lang.ref.SoftReference<GeoMap>> softNavMaps = GeoDataConfig.GEO_NAV_SOFT_CACHE ? new ConcurrentHashMap<>() : null;
    
    /**
     * Lock for map loading when computeIfAbsent can't be used
     * (e.g., for soft reference cache)
     */
    private final ConcurrentHashMap<Integer, ReentrantLock> mapLocks = new ConcurrentHashMap<>();

    private NavData() {}

    /**
     * Checks if navigation data index exists.
     * Does NOT indicate if actual map data is loaded.
     */
    boolean isLoaded() {
        return !navFiles.isEmpty();
    }

    /**
     * Scans for navigation files and builds the file index.
     * Does NOT load actual map data - lazy loading only.
     * Fast startup, minimal memory usage.
     */
    void loadNavMaps() {
        // Skip if pathfinding is globally disabled
        if (!GeoDataConfig.GEO_NAV_ENABLE) {
            LOG.info("Navigation system is disabled, skipping file scan.");
            return;
        }
        
        LOG.info("Scanning for navigation files...");
        long startTime = System.currentTimeMillis();
        int fileCount = 0;
        
        for (WorldMapTemplate map : DataManager.WORLD_MAPS_DATA) {
            int mapId = map.getMapId();
            File navFile = new File(NAV_DIR, mapId + ".nav");
            
            if (navFile.exists() && navFile.isFile()) {
                navFiles.put(mapId, navFile);
                fileCount++;
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Found {} navigation files, took {} ms", fileCount, duration);
    }

    /**
     * Returns the navigation mesh for the specified map.
     * Thread-safe, lazy loading: map is loaded on first access.
     * 
     * @param worldId Map ID
     * @return GeoMap with nav mesh, or null if not available
     */
    public GeoMap getNavMap(int worldId) {
        // Fast path: config check
        if (!GeoDataConfig.GEO_NAV_ENABLE) {
            return null;
        }
        
        // Fast path: soft reference cache if enabled
        if (softNavMaps != null) {
            java.lang.ref.SoftReference<GeoMap> ref = softNavMaps.get(worldId);
            if (ref != null) {
                GeoMap map = ref.get();
                if (map != null) {
                    return map;
                }
                // Reference was cleared, remove stale entry
                softNavMaps.remove(worldId);
            }
        }
        
        // Standard cache with atomic lazy loading
        return navMaps.computeIfAbsent(worldId, this::loadMap);
    }

    /**
     * Internal map loading logic.
     * Called only once per map via computeIfAbsent.
     */
    private GeoMap loadMap(Integer worldId) {
        // Check if file exists for this map
        File navFile = navFiles.get(worldId);
        if (navFile == null) {
            LOG.debug("No navigation file for map {}", worldId);
            return null;
        }
        
        WorldMapTemplate template = DataManager.WORLD_MAPS_DATA.getTemplate(worldId);
        if (template == null) {
            LOG.error("World map template not found for ID: {}", worldId);
            return null;
        }
        
        GeoMap geoMap = new GeoMap(String.valueOf(worldId), template.getWorldSize());
        
        long startTime = System.currentTimeMillis();
        try {
            if (loadNavMesh(worldId, navFile, geoMap)) {
                long duration = System.currentTimeMillis() - startTime;
                LOG.info("Loaded navigation mesh for map {} ({} triangles), took {} ms", worldId, geoMap.getChildren() != null ? geoMap.getChildren().size() : 0, duration);
                
                // Also store in soft cache if enabled
                if (softNavMaps != null) {
                    softNavMaps.put(worldId, new java.lang.ref.SoftReference<>(geoMap));
                }
                
                return geoMap;
            }
        } catch (IOException e) {
            LOG.error("Failed to load navigation file for map {}: {}", worldId, e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("Unexpected error loading navigation for map {}: {}", worldId, e.getMessage(), e);
        }
        
        return null;
    }

    /**
     * Parses .nav file and constructs navigation mesh.
     * 
     * File format:
     * - int: vertexCount
     * - float[vertexCount * 3]: vertex positions (x,y,z)
     * - int: triangleCount
     * - for each triangle:
     *   - int[3]: vertex indices
     *   - int[3]: adjacent triangle indices (-1 if no connection)
     * 
     * @param worldId Map ID for logging
     * @param navFile File to parse
     * @param map GeoMap to populate
     * @return true if loading succeeded
     * @throws IOException on file read errors
     */
    private boolean loadNavMesh(int worldId, File navFile, GeoMap map) throws IOException {
        try (RandomAccessFile raFile = new RandomAccessFile(navFile, "r");
             FileChannel roChannel = raFile.getChannel()) {
            
            MappedByteBuffer nav = roChannel.map(FileChannel.MapMode.READ_ONLY, 0, (int) roChannel.size());
            nav.load();
            nav.order(ByteOrder.LITTLE_ENDIAN);
            
            try {
                // Validate file size
                if (nav.remaining() < LEGACY_HEADER_SIZE_BYTES) {
                    throw new IOException("File too small: missing float count");
                }
                
                // 【修正】2026-03-07 - 读取 floatCount (旧版文件格式)
                // 旧版 .nav 文件头存储的是总浮点数数量，而不是顶点数量
                // floatCount = vertexCount * 3 (每个顶点有3个浮点数：x, y, z)
                int floatCount = nav.getInt();
                if (floatCount <= 0 || floatCount > 3000000) {
                    throw new IOException("Invalid float count: " + floatCount);
                }
                
                // 【修正】2026-03-07 - 计算顶点数量
                int vertexCount = floatCount / VERTEX_COMPONENTS;
                
                // 【修正】2026-03-07 - 跳过顶点数据
                // 旧版格式：顶点数据位置 = 4 (文件头) + floatCount * 4 (浮点数据)
                nav.position(LEGACY_HEADER_SIZE_BYTES + floatCount * FLOAT_SIZE_BYTES);
                
                // Read triangle count
                if (nav.remaining() < FLOAT_SIZE_BYTES) {
                    throw new IOException("Missing triangle count");
                }
                int triangleCount = nav.getInt();
                if (triangleCount <= 0 || triangleCount > 1000000) {
                    throw new IOException("Invalid triangle count: " + triangleCount);
                }
                
                // Parse triangles
                NavGeometry[] triangles = new NavGeometry[triangleCount];
                int[][] connections = new int[triangleCount][3];
                
                for (int i = 0; i < triangleCount; i++) {
                    // Read vertex indices
                    int[] indices = new int[3];
                    indices[0] = nav.getInt();
                    indices[1] = nav.getInt();
                    indices[2] = nav.getInt();
                    
                    // Create triangle geometry
                    triangles[i] = new NavGeometry(null, getVertices(nav, indices));
                    
                    // Read edge connections
                    connections[i][0] = nav.getInt();
                    connections[i][1] = nav.getInt();
                    connections[i][2] = nav.getInt();
                }
                
                // Build adjacency links
                for (int i = 0; i < triangleCount; i++) {
                    if (connections[i][0] != -1) 
                        triangles[i].setEdge1(triangles[connections[i][0]]);
                    if (connections[i][1] != -1) 
                        triangles[i].setEdge2(triangles[connections[i][1]]);
                    if (connections[i][2] != -1) 
                        triangles[i].setEdge3(triangles[connections[i][2]]);
                    
                    triangles[i].updateModelBound();
                    map.attachChild(triangles[i]);
                }
                
                map.updateModelBound();
                
            } finally {
                // Always release native buffer, even on error
                releaseDirectBuffer(nav);
            }
        }
        
        return true;
    }

    /**
     * Extracts vertex coordinates from the buffer.
     * Uses legacy format offset calculation.
     * 
     * @param nav Mapped buffer containing vertex data
     * @param indices Indices of vertices to extract
     * @return Array of vertex coordinates [x,y,z, x,y,z, ...]
     */
    private static float[] getVertices(MappedByteBuffer nav, int[] indices) {
        float[] ret = new float[indices.length * VERTEX_COMPONENTS];
        for (int i = 0; i < indices.length; i++) {
            // 【修正】2026-03-07 - 旧版格式顶点偏移计算
            // 顶点数据从文件头后开始：offset = 4 (文件头) + (顶点索引 * 3 * 4) + (分量偏移)
            ret[i * VERTEX_COMPONENTS] = nav.getFloat((indices[i] * FLOAT_SIZE_BYTES * VERTEX_COMPONENTS) + FLOAT_SIZE_BYTES);
            ret[(i * VERTEX_COMPONENTS) + 1] = nav.getFloat((indices[i] * FLOAT_SIZE_BYTES * VERTEX_COMPONENTS) + (FLOAT_SIZE_BYTES * 2));
            ret[(i * VERTEX_COMPONENTS) + 2] = nav.getFloat((indices[i] * FLOAT_SIZE_BYTES * VERTEX_COMPONENTS) + (FLOAT_SIZE_BYTES * 3));
        }
        return ret;
    }

    /**
     * Safely releases a direct byte buffer's native memory.
     * Uses reflection to avoid sun.misc.Cleaner dependency.
     */
    private static void releaseDirectBuffer(Buffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }
        
        try {
            // Try Java 9+ approach first
            if (System.getProperty("java.version").startsWith("1.")) {
                // Java 8 - use sun.misc.Cleaner
                try {
                    Class<?> directBufferClass = Class.forName("sun.nio.ch.DirectBuffer");
                    Method cleanerMethod = directBufferClass.getMethod("cleaner");
                    cleanerMethod.setAccessible(true);
                    Object cleaner = cleanerMethod.invoke(buffer);
                    if (cleaner != null) {
                        Method cleanMethod = cleaner.getClass().getMethod("clean");
                        cleanMethod.invoke(cleaner);
                    }
                } catch (Exception ignored) {
                    // Fallback: just let GC handle it
                }
            } else {
                // Java 9+ - use invoke-exact or let GC handle
                try {
                    Method attachmentMethod = buffer.getClass().getMethod("attachment");
                    attachmentMethod.setAccessible(true);
                    Object attachment = attachmentMethod.invoke(buffer);
                    
                    if (attachment == null) {
                        Method cleanerMethod = buffer.getClass().getMethod("cleaner");
                        cleanerMethod.setAccessible(true);
                        Object cleaner = cleanerMethod.invoke(buffer);
                        if (cleaner != null) {
                            Method cleanMethod = cleaner.getClass().getMethod("clean");
                            cleanMethod.invoke(cleaner);
                        }
                    }
                } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            LOG.debug("Failed to release direct buffer: {}", e.getMessage());
        }
    }

    /**
     * Removes a specific map from the cache to free memory.
     * Useful for memory management in long-running servers.
     * 
     * @param worldId Map ID to clear
     */
    public void clearNavMap(int worldId) {
        GeoMap removed = navMaps.remove(worldId);
        if (removed != null) {
            removed.detachAllChildren();
            LOG.debug("Cleared navigation cache for map {}", worldId);
        }
        
        if (softNavMaps != null) {
            softNavMaps.remove(worldId);
        }
    }

    /**
     * Clears all navigation maps from cache.
     */
    public void clearAllNavMaps() {
        navMaps.clear();
        if (softNavMaps != null) {
            softNavMaps.clear();
        }
        LOG.info("Cleared all navigation caches");
    }

    /**
     * Returns the file index size (number of available nav meshes).
     */
    public int getAvailableMapCount() {
        return navFiles.size();
    }

    /**
     * Returns the number of currently loaded nav meshes.
     */
    public int getLoadedMapCount() {
        return navMaps.size();
    }

    /**
     * Singleton holder.
     */
    public static NavData getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private static final class SingletonHolder {
        protected static final NavData INSTANCE = new NavData();
    }
}