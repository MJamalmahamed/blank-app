import streamlit as st
import pandas as pd
import pyodbc
import plotly.express as px
from datetime import datetime

# Page config
st.set_page_config(page_title="Modern DBA Dashboard", layout="wide")

# Sidebar for login
st.sidebar.title("SQL Server Login")
server = st.sidebar.text_input("Server", value="localhost")
username = st.sidebar.text_input("Username")
password = st.sidebar.text_input("Password", type="password")
connect_button = st.sidebar.button("Connect")

conn = None


def connect_sql(server, username, password):
    try:
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password}"
        )
        return connection
    except Exception as e:
        st.sidebar.error(f"Connection failed: {e}")
        return None


def execute_query(conn, query):
    try:
        df = pd.read_sql_query(query, conn)
        df.columns = df.columns.str.strip().str.lower()  # Normalize column names
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


if connect_button:
    conn = connect_sql(server, username, password)
    if conn:
        st.success("Connected successfully!")

        tabs = st.tabs(
            [
                "KPI & Alerts",
                "Storage",
                "Backups",
                "Jobs",
                "Indexes",
                "Top Queries",
                "Sessions",
                "TempDB",
                "CPU & Memory",
                "Waits",
                "Alerts",
                "Advanced Insights",
            ]
        )

        # === KPI & Alerts ===
        with tabs[0]:
            st.header("Daily Health Check KPIs")

            # SQL Server Configuration (added)
            st.subheader("SQL Server Configuration")
            df_config = execute_query(conn, "EXEC sp_configure")
            if not df_config.empty:
                st.dataframe(df_config[["name", "config_value", "run_value"]])
            else:
                st.dataframe(df_config[["name", "config_value", "run_value"]])
            # Disk Free Space
            df_space = execute_query(conn, "EXEC xp_fixeddrives")
            if not df_space.empty:
                for _, row in df_space.iterrows():
                    free_col = "mb free" if "mb free" in df_space.columns else "free"
                    st.metric(
                        label=f"Drive {row['drive']} Free Space (MB)",
                        value=row[free_col] * 1.0,
                    )
                    if row[free_col] < 10240:
                        st.warning(f"Low space on drive {row['drive']} (<10GB)")

            # Last Backup
            df_back = execute_query(
                conn,
                """
                SELECT 
                    d.name AS databasename,
                    MAX(b.backup_finish_date) AS lastbackupdate
                FROM msdb.dbo.backupset b
                JOIN sys.databases d ON d.name = b.database_name
                GROUP BY d.name
            """,
            )
            if not df_back.empty:
                now = datetime.now()
                for _, row in df_back.iterrows():
                    if row["lastbackupdate"]:
                        hours = (now - row["lastbackupdate"]).total_seconds() / 3600
                        st.metric(
                            f"{row['databasename']} Last Backup (h)", round(hours, 1)
                        )
                        if hours > 24:
                            st.error(f"Backup overdue for {row['databasename']}")

        # === Storage ===
        with tabs[1]:
            st.header("Database Storage")
            df = execute_query(
                conn,
                """
                SELECT 
                    d.name AS databasename,
                    mf.name AS filename,
                    mf.physical_name,
                    mf.size * 8 / 1024 AS sizemb,
                    mf.type_desc
                FROM sys.master_files mf
                JOIN sys.databases d ON d.database_id = mf.database_id;
            """,
            )
            if not df.empty:
                fig = px.pie(
                    df,
                    names="databasename",
                    values="sizemb",
                    title="Storage by Database",
                )
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df)

        # === Backups ===
        with tabs[2]:
            st.header("Backup Timeline")
            df = execute_query(
                conn,
                """
                SELECT 
                    database_name,
                    backup_finish_date,
                    CASE type WHEN 'D' THEN 'Full' WHEN 'L' THEN 'Log' WHEN 'I' THEN 'Diff' ELSE type END AS backuptype
                FROM msdb.dbo.backupset
                WHERE backup_finish_date > GETDATE() - 7
            """,
            )
            if not df.empty:
                fig = px.scatter(
                    df,
                    x="backup_finish_date",
                    y="database_name",
                    color="backuptype",
                    title="Backups in Last 7 Days",
                )
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df)

        # === Jobs ===
        with tabs[3]:
            st.header("SQL Agent Jobs")
            df = execute_query(
                conn,
                """
                SELECT 
                    sj.name AS jobname,
                    CASE sj.enabled WHEN 1 THEN 'Yes' ELSE 'No' END AS enabled,
                    h.run_date,
                    h.run_status,
                    h.run_duration
                FROM msdb.dbo.sysjobs sj
                LEFT JOIN msdb.dbo.sysjobhistory h ON sj.job_id = h.job_id AND h.step_id = 0
                WHERE h.run_date >= CONVERT(VARCHAR, GETDATE()-7, 112);
            """,
            )
            if not df.empty:
                df["status"] = df["run_status"].map(
                    {0: "Failed", 1: "Succeeded", 2: "Retry", 3: "Canceled"}
                )
                fig = px.histogram(
                    df, x="jobname", color="status", title="Job Runs in Last 7 Days"
                )
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df)

        # === Indexes ===
        with tabs[4]:
            st.header("Index Health")
            df = execute_query(
                conn,
                """
                SELECT 
                    DB_NAME(ius.database_id) AS Database_Name,
                    OBJECT_NAME(ius.object_id, ius.database_id) AS Object_Name,
                    ius.object_id AS Object_ID,
                    ius.database_id AS Database_ID,
                    i.name AS Index_Name,
                    ius.user_seeks,
                    ius.user_scans,
                    ius.user_lookups,
                    ius.user_updates,
                    ius.last_user_seek,
                    ius.last_user_scan,
                    ius.last_user_lookup,
                    ius.last_user_update
                FROM sys.dm_db_index_usage_stats AS ius
                JOIN sys.indexes AS i 
                ON i.index_id = ius.index_id 
                AND i.object_id = ius.object_id
                WHERE ius.database_id = DB_ID() 
                AND ius.user_seeks = 0 
                AND ius.user_scans = 0 
                AND ius.user_lookups = 0
                ORDER BY ius.last_user_update DESC;
                 """,
            )
            if not df.empty:
                df = df.sort_values(by="avg_fragmentation_in_percent", ascending=False)
                st.dataframe(df)

            df_missing = execute_query(
                conn,
                """
                SELECT 
                    DB_NAME(mid.database_id) AS dbname,
                    mid.statement AS tablestatement,
                    mid.equality_columns,
                    mid.inequality_columns,
                    mid.included_columns,
                    migs.avg_user_impact,
                    migs.user_seeks,
                    migs.user_scans
                FROM sys.dm_db_missing_index_group_stats migs
                JOIN sys.dm_db_missing_index_groups mig ON migs.group_handle = mig.index_group_handle
                JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
                ORDER BY migs.avg_user_impact DESC;
            """,
            )
            if not df_missing.empty:
                st.subheader("Missing Indexes")
                st.dataframe(df_missing)

            # Added: Unused Indexes Insight
            st.subheader("Unused Indexes")
            df_unused_indexes = execute_query(
                conn,
                """
                SELECT 
    DB_NAME(ius.database_id) AS Database_Name,
    OBJECT_NAME(ius.object_id, ius.database_id) AS Object_Name,
    ius.object_id AS Object_ID,
    ius.database_id AS Database_ID,
    i.name AS Index_Name,
    ius.user_seeks,
    ius.user_scans,
    ius.user_lookups,
    ius.user_updates,
    ius.last_user_seek,
    ius.last_user_scan,
    ius.last_user_lookup,
    ius.last_user_update
FROM sys.dm_db_index_usage_stats AS ius
JOIN sys.indexes AS i 
    ON i.index_id = ius.index_id 
    AND i.object_id = ius.object_id
WHERE ius.database_id = DB_ID() 
    AND ius.user_seeks = 0 
    AND ius.user_scans = 0 
    AND ius.user_lookups = 0
ORDER BY ius.last_user_update DESC;
            """,
            )
            if not df_unused_indexes.empty:
                st.dataframe(df_unused_indexes)

        # === Top Queries ===
        with tabs[5]:
            st.header("Top Resource-Consuming Queries")
            df = execute_query(
                conn,
                """
                SELECT TOP 20
                    qs.execution_count,
                    qs.total_elapsed_time / 1000 AS totalms,
                    qs.total_logical_reads,
                    st.text AS querytext
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                ORDER BY qs.total_elapsed_time DESC;
            """,
            )
            if not df.empty:
                st.dataframe(df)
                st.caption("Sorted by total execution time")

            # Added: Long Running Queries (> 5 min)
            st.subheader("Long Running Queries (> 5 minutes)")
            df_long_queries = execute_query(
                conn,
                """
                SELECT TOP 20
                    qs.total_elapsed_time / 1000 AS total_ms,
                    qs.execution_count,
                    qs.total_elapsed_time / qs.execution_count / 1000 AS avg_exec_ms,
                    st.text AS query_text,
                    DB_NAME(st.dbid) AS database_name
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                WHERE qs.total_elapsed_time / 1000 > 300000  -- > 5 minutes
                ORDER BY total_elapsed_time DESC;
            """,
            )
            if not df_long_queries.empty:
                st.dataframe(df_long_queries)
            else:
                st.info("No long running queries found.")

        # === Sessions ===
        with tabs[6]:
            st.header("Active Sessions")
            df = execute_query(
                conn,
                """
                SELECT 
                    session_id, 
                    login_name, 
                    status, 
                    cpu_time, 
                    memory_usage, 
                    total_elapsed_time
                FROM sys.dm_exec_sessions
                WHERE is_user_process = 1;
            """,
            )
            if not df.empty:
                st.dataframe(df)

        # === TempDB ===
        with tabs[7]:
            st.header("TempDB Usage & Contention")
            df_tempdb = execute_query(
                conn,
                """
                SELECT
                    SUM(unallocated_extent_page_count) AS unallocated_pages,
                    SUM(version_store_reserved_page_count) AS version_store_pages,
                    SUM(user_object_reserved_page_count) AS user_object_pages,
                    SUM(internal_object_reserved_page_count) AS internal_object_pages,
                    SUM(mixed_extent_page_count) AS mixed_extent_pages
                FROM sys.dm_db_file_space_usage;
            """,
            )
            if not df_tempdb.empty:
                st.dataframe(df_tempdb)

            # Added: TempDB Contention Latch Waits
            st.subheader("TempDB Contention Waits")
            df_tempdb_contention = execute_query(
                conn,
                """
                SELECT
                    wait_type,
                    waiting_tasks_count,
                    wait_time_ms,
                    max_wait_time_ms,
                    signal_wait_time_ms
                FROM sys.dm_os_wait_stats
                WHERE wait_type LIKE 'PAGELATCH_%' OR wait_type LIKE 'PAGEIOLATCH_%'
                ORDER BY wait_time_ms DESC;
            """,
            )
            if not df_tempdb_contention.empty:
                st.dataframe(df_tempdb_contention)

        # === CPU & Memory ===
        with tabs[8]:
            st.header("CPU & Memory Usage Snapshot")

            # Ring Buffer CPU Utilization Timeline Query
            cpu_timeline_query = """
            DECLARE @ts_now bigint;  
            SET @ts_now = (SELECT ms_ticks FROM sys.dm_os_sys_info);

            SELECT TOP (256)
                y.SQL_Process_Utilization AS [SQL Server Process CPU Utilization],
                y.System_Idle AS [System Idle Process],
                100 - y.System_Idle - y.SQL_Process_Utilization AS [Other Process CPU Utilization],
                DATEADD(ms, -1 * (@ts_now - y.[timestamp]), GETDATE()) AS [Event Time]
            FROM (
                SELECT 
                    record.value('(./Record/@id)[1]', 'int') AS record_id,
                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS System_Idle,
                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQL_Process_Utilization,
                    [timestamp]
                FROM (
                    SELECT 
                        [timestamp], 
                        CONVERT(xml, record) AS record
                    FROM sys.dm_os_ring_buffers WITH (NOLOCK)
                    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
                        AND record LIKE N'%<SystemHealth>%'
                ) AS x
            ) AS y
            ORDER BY y.record_id DESC
            OPTION (RECOMPILE);
            """

            df_cpu_timeline = execute_query(conn, cpu_timeline_query)
            if not df_cpu_timeline.empty:
                fig = px.line(
                    df_cpu_timeline,
                    x="event time",
                    y=[
                        "sql server process cpu utilization",
                        "system idle process",
                        "other process cpu utilization",
                    ],
                    title="CPU Utilization Timeline",
                )
                st.plotly_chart(fig, use_container_width=True)

            # Memory Usage Query
            memory_query = """
            SELECT 
                total_physical_memory_kb / 1024 AS TotalPhysicalMemoryMB,
                available_physical_memory_kb / 1024 AS AvailablePhysicalMemoryMB,
                (total_physical_memory_kb - available_physical_memory_kb) / 1024 AS UsedPhysicalMemoryMB,
                system_memory_state_desc AS MemoryState
            FROM sys.dm_os_sys_memory;
            """
            df_mem = execute_query(conn, memory_query)
            if not df_mem.empty:
                mem = df_mem.iloc[0]
                st.metric("Total Physical Memory (MB)", mem["totalphysicalmemorymb"])
                st.metric(
                    "Available Physical Memory (MB)", mem["availablephysicalmemorymb"]
                )
                st.metric("Used Physical Memory (MB)", mem["usedphysicalmemorymb"])
                st.text(f"Memory State: {mem['memorystate']}")

        # === Waits ===
        with tabs[9]:
            st.header("Wait Statistics")
            df_waits = execute_query(
                conn,
                """
                SELECT TOP 20
                    wait_type,
                    waiting_tasks_count,
                    wait_time_ms,
                    max_wait_time_ms,
                    signal_wait_time_ms
                FROM sys.dm_os_wait_stats
                ORDER BY wait_time_ms DESC;
            """,
            )
            if not df_waits.empty:
                st.dataframe(df_waits)

        # === Alerts ===
        with tabs[10]:
            st.header("Alerts & Notifications")
            # Implement your alerting system here or display recent alerts from a table
            st.info("Alerts system is under development.")

        # === Advanced Insights ===
        with tabs[11]:
            st.header("Advanced Insights")

            # Deadlocks last 7 days
            st.subheader("Recent Deadlocks (Last 7 Days)")
            df_deadlocks = execute_query(
                conn,
                """
                SELECT 
                    XEventData.value('(event/@timestamp)[1]', 'datetime') AS deadlock_time,
                    XEventData.query('.') AS deadlock_graph
                FROM (
                    SELECT CAST(target_data AS XML) AS TargetData
                    FROM sys.dm_xe_session_targets st
                    JOIN sys.dm_xe_sessions s ON s.address = st.event_session_address
                    WHERE s.name = 'system_health' AND st.target_name = 'ring_buffer'
                ) AS Data
                CROSS APPLY TargetData.nodes('RingBufferTarget/Event[@name="xml_deadlock_report"]') AS XEvent(XEventData)
                WHERE XEventData.value('(event/@timestamp)[1]', 'datetime') > DATEADD(day, -7, GETDATE())
                ORDER BY deadlock_time DESC;
            """,
            )
            if not df_deadlocks.empty:
                st.dataframe(df_deadlocks)
            else:
                st.info("No deadlocks found in last 7 days.")

            # You can add more advanced insights here if needed

else:
    st.info("Please enter your SQL Server credentials and click Connect.")
