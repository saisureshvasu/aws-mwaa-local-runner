
class DenodoModel:
    def __init__(self, job_config_id, job_id, project_id, job_name, view_name):
        self.job_config_id = job_config_id
        self.job_id = job_id
        self.project_id = project_id
        self.job_name = job_name
        self.view_name = view_name
        self.dag_list = list()
        self.source_tables = set()

    def __repr__ (self):
        return """DenodoModel(
            job_config_id = {job_config_id},
            job_id = {job_id},
            project_id = {project_id} ,
            job_name = {job_name},
            view_name = {view_name},
            dags_list = {dags_list},
            source_tables = {source_tables})""".format(
                job_config_id=self.job_config_id
                , job_id=str(self.job_id)
                , project_id=str(self.project_id)
                , job_name=str(self.job_name)
                , view_name=self.view_name
                , dags_list=str(self.dag_list)
                , source_tables=str(self.source_tables) )