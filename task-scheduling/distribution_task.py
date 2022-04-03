# -*- coding: UTF-8 -*-
# 导入airflow需要的modules
import datetime
from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator


def get_yesterday():
    today = datetime.date.today()
    one_day = datetime.timedelta(days=1)
    return (today - one_day).strftime('%Y-%m-%d')


dag_id = "distribution-dag"
# 实例化DAG图
with DAG(
        dag_id=dag_id,
        # 1.定义默认参数
        default_args={
            'owner': 'pingcai',
            'depends_on_past': False,  # 如上文依赖关系所示
            'start_date': datetime(2022, 4, 3),  # DAGs都有个参数start_date，表示调度器调度的起始时间
            'email': ['pingcai2022@163.com'],  # 用于alert
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 0,  # 重试策略
            'provide_context': True
        },
        description="客流分配任务调度",
        schedule_interval="0 18 * * *",
        start_date=datetime(2022, 4, 3),
        catchup=False,
        tags=['distribution']
)as dag:
    migration_command = '''
    cd $PING_CAI_HOME/rail-transit/sicau-rail_transit-1.0-SNAPSHOT-bin/bin && sh mysql-to-hdfs-migration.sh "%s"
    ''' % (get_yesterday())
    # 定义执行顺序
    t1 = BashOperator(task_id="migration_task",
                      bash_command=migration_command)
    afc_extra_command = '''
    cd $PING_CAI_HOME/rail-transit/sicau-rail_transit-1.0-SNAPSHOT-bin/bin && sh afc-extra.sh "%s"
    ''' % (get_yesterday())
    t2 = BashOperator(task_id="afc_extra_task",
                      bash_command=afc_extra_command)
    afc_pair_agg_command = '''
    cd $PING_CAI_HOME/rail-transit/sicau-rail_transit-1.0-SNAPSHOT-bin/bin && sh afc-pair-agg.sh "%s"
    ''' % (get_yesterday())
    t3 = BashOperator(task_id="afc_pair_agg_task",
                      bash_command=afc_pair_agg_command)
    static_distribution_command = '''
    cd $PING_CAI_HOME/rail-transit/sicau-rail_transit-1.0-SNAPSHOT-bin/bin && sh static-distribution.sh "%s"
    ''' % (get_yesterday())
    t4 = BashOperator(task_id="static_distribution_task",
                      bash_command=static_distribution_command)
    test_email_command = "cd $PING_CAI_HOME/rail-transit/sicau-rail_transit-1.0-SNAPSHOT-bin/bin && sh email.sh"
    t5 = BashOperator(task_id="email_test_task",
                      bash_command=test_email_command)
    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    
    """
    )
    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    template_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )
    t1 >> t2 >> t3 >> t4 >> t5
