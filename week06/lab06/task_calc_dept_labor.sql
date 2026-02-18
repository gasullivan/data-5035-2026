/*******************************************************************************
 * Task: TASK_CALC_DEPT_LABOR
 * Schema: DATA5035.INSTRUCTOR1
 * 
 * Root task in the budget monitoring task graph.
 * Scheduled to run daily at 6:00 AM Central Time.
 * 
 * Calls CALC_DEPT_LABOR to calculate prorated department labor costs
 * for the current year.
 * 
 * Task Graph:
 *   TASK_CALC_DEPT_LABOR (this task - root)
 *           |
 *           v
 *   TASK_CALC_DEPT_TO_BUDGET
 *           |
 *           v
 *   TASK_NOTIFY_DEPT
 ******************************************************************************/

<<<<<<< local

CREATE OR REPLACE TASK SNOWBEARAIR_DB.GSULLIVAN.TASK_CALC_DEPT_LABOR
=======
CREATE OR REPLACE TASK DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_LABOR
>>>>>>> remote
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    SCHEDULE = 'USING CRON 0 6 * * * America/Chicago'
AS
<<<<<<< local
    CALL SNOWBEARAIR_DB.GSULLIVAN.CALC_DEPT_LABOR(
=======
    CALL DATA5035.INSTRUCTOR1.CALC_DEPT_LABOR(
>>>>>>> remote
        'DATA5035.SPRING26.EMPLOYEES',
        'DATA5035.SPRING26.POSITIONS',
        YEAR(CURRENT_DATE()),
<<<<<<< local
        'SNOWBEARAIR_DB.GSULLIVAN.DEPT_LABOR_ACTUAL'
=======
        'DATA5035.INSTRUCTOR1.DEPT_LABOR_ACTUAL'
>>>>>>> remote
    );

<<<<<<< local
    
=======
>>>>>>> remote
-- To enable the task graph, run:
-- ALTER TASK DATA5035.INSTRUCTOR1.TASK_NOTIFY_DEPT RESUME;
-- ALTER TASK DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_TO_BUDGET RESUME;
-- ALTER TASK DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_LABOR RESUME;

-- To manually execute the task graph:
-- EXECUTE TASK DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_LABOR;
