find_report_name_sql = """ SELECT report_name
                       FROM postgres.admin.report_configuration
                       WHERE report_name=(%s)"""

insert_into_report_configuration_sql = """INSERT INTO postgres.admin.report_configuration(report_name, created_by, created_timestamp)
                                          VALUES(%s, %s, %s)"""

find_report_id_sql = """SELECT report_id FROM postgres.admin.report_configuration
                        WHERE report_name=(%s) """

insert_into_report_schedule_sql = """INSERT INTO postgres.admin.report_schedule(report_id, run_date, begin_date, end_date, status, next_run_formula, is_active, created_timestamp)
                                     VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"""

insert_into_report_definition_sql = """INSERT INTO postgres.admin.report_definition(report_id, sql_path, output_filename, date_format, delimiter, output_file_location, created_timestamp)
                                       VALUES(%s, %s, %s, %s, %s, %s, %s)"""

update_report_configuration_sql = """UPDATE postgres.admin.report_configuration
                                     SET updated_by=(%s),
                                         updated_timestamp=(%s)
                                     WHERE report_name=(%s)"""

update_report_schedule_sql = """UPDATE postgres.admin.report_schedule
                                SET run_date=(%s),
                                    begin_date=(%s), 
                                    end_date=(%s), 
                                    status=(%s),
                                    next_run_formula=(%s),
                                    is_active=(%s),
                                    updated_timestamp=(%s)
                                WHERE report_id=(%s)"""

update_report_definition_sql = """UPDATE postgres.admin.report_definition
                                  SET sql_path=(%s), 
                                      output_filename=(%s),
                                      date_format=(%s),
                                      delimiter=(%s), 
                                      output_file_location=(%s), 
                                      updated_timestamp=(%s) 
                                  WHERE report_id=(%s)"""

find_queued_schedules_sql = """SELECT rc.report_name,
                                      rs.run_date,
                                      rs.begin_date,
                                      rs.end_date,
                                      rd.sql_path,
                                      rd.output_filename,
                                      rd.date_format,
                                      rd.delimiter,
                                      rd.output_file_location,
                                      rc.report_id,
                                      rs.next_run_formula,
                                      rs.status
                                FROM postgres.admin.report_configuration rc
                                INNER JOIN postgres.admin.report_schedule rs ON rc.report_id = rs.report_id 
                                INNER JOIN postgres.admin.report_definition rd ON rc.report_id = rd.report_id 
                                AND rs.is_active = True
                                AND rs.run_date <= current_date"""

update_dates_for_next_run_sql = """UPDATE postgres.admin.report_schedule
                                   SET run_date=(%s),
                                       begin_date=(%s), 
                                       end_date=(%s) 
                                   WHERE report_id=(%s)"""

set_schedule_inactive_sql = """UPDATE postgres.admin.report_schedule
                               SET is_active = FALSE
                               WHERE report_id=(%s)"""
                            
set_schedule_active_sql = """UPDATE postgres.admin.report_schedule
                             SET is_active = TRUE
                             WHERE report_id=(%s)"""

insert_into_report_run_sql = """INSERT INTO postgres.admin.report_run(report_id, run_date, begin_date, end_date, status, next_run_formula, sql_path, output_filename, output_file_location)
                                     VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"""