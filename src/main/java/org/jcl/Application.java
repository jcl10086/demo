package org.jcl;/**
 * Created by admin on 2018/7/20.
 */

import org.jcl.core.HbaseSpark;
import org.jcl.core.HbaseTest;
import org.jcl.core.SparkSqlHive;
import org.jcl.core.WordCount;

/**
 * @author jichenglu
 * @create 2018-07-20 16:47
 * 启动函数
 **/
public class Application {

    public static void main(String[] args) {

        /**
         * 本地：HbaseSpark ccu_data local "2018-01-01 00:00:00" "2018-01-02 00:00:00"
         * 集群：HbaseSpark ccu_data yarn "2018-01-01 00:00:00" "2018-01-02 00:00:00"
         */
        if("HbaseSpark".equals(args[0])){

            String table=args[1];

            String master=args[2];

            String st=args[3];

            String et=args[4];

            HbaseSpark.startJob(table,master,st,et);
        }

        /**
         * 本地: HbaseTest ccu_data local
         * 集群: HbaseTest ccu_data yarn
         */
        if("HbaseTest".equals(args[0])){

            String table=args[1];

            String master=args[2];

            HbaseTest.startJob(table,master);
        }

        /**
         * 本地: SparkSqlHive local '"2018-01-01 00:00:00"' '"2018-01-02 00:00:00"'
         * 集群: SparkSqlHive yarn '"2018-01-01 00:00:00"' '"2018-01-02 00:00:00"'
         */
        if("SparkSqlHive".equals(args[0])){

            String master=args[1];

            String st=args[2];

            String et=args[3];

            SparkSqlHive.startJob(master,st,et);

        }

        /**
         * 本地: WordCount local
         * 集群: WordCount yarn
         */
        if("WordCount".equals(args[0])){

            String master=args[1];

            WordCount.startJob(master);
        }
    }
}
