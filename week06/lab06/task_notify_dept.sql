/*******************************************************************************
 * Task: TASK_NOTIFY_DEPT
 * Schema: DATA5035.INSTRUCTOR1
 * 
 * Final task in the budget monitoring task graph.
 * Runs after TASK_CALC_DEPT_TO_BUDGET completes.
 * 
 * Calls NOTIFY_DEPT to generate email notifications for any departments
 * that are over budget.
 * 
 * Task Graph:
 *   TASK_CALC_DEPT_LABOR
 *           |
 *           v
 *   TASK_CALC_DEPT_TO_BUDGET
 *           |
 *           v
 *   TASK_NOTIFY_DEPT (this task)
 ******************************************************************************/

<<<<<<< local
CREATE OR REPLACE TASK SNOWBEARAIR_DB.GSULLIVAN.TASK_NOTIFY_DEPT
=======
CREATE OR REPLACE TASK DATA5035.INSTRUCTOR1.TASK_NOTIFY_DEPT
>>>>>>> remote
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
<<<<<<< local
    AFTER SNOWBEARAIR_DB.GSULLIVAN.TASK_CALC_DEPT_TO_BUDGET
=======
    AFTER DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_TO_BUDGET
>>>>>>> remote
AS
<<<<<<< local
    CALL SNOWBEARAIR_DB.GSULLIVAN.NOTIFY_DEPT(
        'SNOWBEARAIR_DB.GSULLIVAN.DEPT_BUDGET_STATUS',
=======
    CALL DATA5035.INSTRUCTOR1.NOTIFY_DEPT(
        'DATA5035.INSTRUCTOR1.DEPT_BUDGET_STATUS',
>>>>>>> remote
        'pboal@wustl.edu',
<<<<<<< local
        'SNOWBEARAIR_DB.GSULLIVAN.DEPT_NOTIFICATIONS'
=======
        'DATA5035.INSTRUCTOR1.DEPT_NOTIFICATIONS'
>>>>>>> remote
    );
