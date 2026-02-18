/*******************************************************************************
 * Task: TASK_CALC_DEPT_TO_BUDGET
 * Schema: DATA5035.INSTRUCTOR1
 * 
 * Second task in the budget monitoring task graph.
 * Runs after TASK_CALC_DEPT_LABOR completes.
 * 
 * Calls CALC_DEPT_TO_BUDGET to compare actual labor costs against
 * department budgets and determine OVER/UNDER/MATCHED status.
 * 
 * Task Graph:
 *   TASK_CALC_DEPT_LABOR
 *           |
 *           v
 *   TASK_CALC_DEPT_TO_BUDGET (this task)
 *           |
 *           v
 *   TASK_NOTIFY_DEPT
 ******************************************************************************/

<<<<<<< local
CREATE OR REPLACE TASK SNOWBEARAIR_DB.GSULLIVAN.TASK_CALC_DEPT_TO_BUDGET
=======
CREATE OR REPLACE TASK DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_TO_BUDGET
>>>>>>> remote
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
<<<<<<< local
    AFTER SNOWBEARAIR_DB.GSULLIVAN.TASK_CALC_DEPT_LABOR
=======
    AFTER DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_LABOR
>>>>>>> remote
AS
<<<<<<< local
    CALL SNOWBEARAIR_DB.GSULLIVAN.CALC_DEPT_TO_BUDGET(
        'SNOWBEARAIR_DB.GSULLIVAN.DEPT_LABOR_ACTUAL',
=======
    CALL DATA5035.INSTRUCTOR1.CALC_DEPT_TO_BUDGET(
        'DATA5035.INSTRUCTOR1.DEPT_LABOR_ACTUAL',
>>>>>>> remote
        'DATA5035.SPRING26.DEPARTMENTS',
<<<<<<< local
        'SNOWBEARAIR_DB.GSULLIVAN.DEPT_BUDGET_STATUS'
=======
        'DATA5035.INSTRUCTOR1.DEPT_BUDGET_STATUS'
>>>>>>> remote
    );
