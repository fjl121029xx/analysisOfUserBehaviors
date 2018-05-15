package com.li.spark;

import com.alibaba.fastjson.JSONObject;
import com.li.conf.ConfigurationManager;
import com.li.constant.Constants;
import com.li.dao.ITaskDAO;
import com.li.dao.factory.DAOFactory;
import com.li.domain.Task;
import com.li.util.MockData;
import com.li.util.ParamUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;


/**
 * 用户访问session分析Spark作业
 *
 * @author Administrator
 */

/**
 * 用户访问session分析Spark作业
 * <p>
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * <p>
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * <p>
 * 我们的spark作业如何接受用户创建的任务？
 * <p>
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 * <p>
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 * <p>
 * 这是spark本身提供的特性
 *
 * @author Administrator
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        // 构建Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());

        // 生成模拟测试数据
        mockData(sc, sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        // 首先得查询出来指定的任务，并获取任务的查询参数
        Long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);
        JSONObject taskparm = JSONObject.parseObject(task.getTaskParam());

        JavaRDD<Row> actionRDD = getActionRddByDataRange(sqlContext, taskparm);

        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregaBySession(sqlContext, actionRDD);

        // 关闭Spark上下文
        sc.close();
    }


    private static JavaPairRDD<String, String> aggregaBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {

        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        }).groupByKey();

        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                String sessionId = tuple._1;
                Iterator<Row> iterator = tuple._2.iterator();

                StringBuilder searchKeywordBuffer = new StringBuilder("");
                StringBuilder clickCategoryIdBuffer = new StringBuilder("");

                Long userid = null;

                while (iterator.hasNext()) {

                    Row row = iterator.next();
                    String searchKeyword = row.getString(5);
                    Long clickCategoryid = row.getLong(6);

                    if (StringUtils.isNoneEmpty(searchKeyword)) {
                        if (!searchKeywordBuffer.toString().contains(searchKeyword)) {
                            searchKeywordBuffer.append(searchKeyword);
                        }
                    }
                    if (clickCategoryid != null) {
                        if (!clickCategoryIdBuffer.toString().contains(clickCategoryid.toString())) {
                            clickCategoryIdBuffer.append(clickCategoryid);
                        }
                    }
                }

                String searchkeywords = com.li.util.StringUtils.trimComma(searchKeywordBuffer.toString());
                String clickCategoryIds = com.li.util.StringUtils.trimComma(clickCategoryIdBuffer.toString());

                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchkeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;

                return new Tuple2<>(userid, partAggrInfo);
            }
        });

        String sql = "SELECT * FROM user_info";
        JavaRDD<Row> userid2infoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userid2infoRDD.mapToPair(new PairFunction<Row, Long, Row>() {

            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });

        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {

                String partAggrInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;


                String sessionid = com.li.util.StringUtils.getFieldFromConcatString(partAggrInfo, "|", Constants.FIELD_SESSION_ID);

                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggrInfo = partAggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<>(sessionid, fullAggrInfo);

            }
        });

        return sessionid2FullAggrInfoRDD;
    }

    private static JavaRDD<Row> getActionRddByDataRange(SQLContext sqlContext, JSONObject taskparm) {


        String startDate = ParamUtils.getParam(taskparm, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskparm, Constants.PARAM_END_DATE);

        String sql = " " +
                "SELECT * " +
                " FROM user_visit_action " +
                "WHERE date >= '" + startDate + "' " +
                "AND date <= '" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD();

    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

}
