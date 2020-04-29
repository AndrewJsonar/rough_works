input_collections = [system_info, dm_extraction_log, stap_status, datasources, buff_usage, access_log_detailed, va, user_role, sfe_bundle, policy_violations, classifier, full_sql, group_members, installed_patches, policy_violations_detailed, session_log, exception_log]
g2_data_sources = [gibm32, gibm34, gibm38, china2, gva04, ghp02]


{ $out:{"fstype": "local", "format": "csv", "filename": "foo" }}