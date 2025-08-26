
<create_file path="/home/ubuntu/repos/mara-example-project-1/microservices/data_loader/sql/recreate_marketing_data_schema.sql">
DROP SCHEMA IF EXISTS m_data CASCADE;
CREATE SCHEMA m_data;

SELECT util.create_chunking_functions('m_data');
