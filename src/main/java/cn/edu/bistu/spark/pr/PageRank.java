/**
 * 
 */
package cn.edu.bistu.spark.pr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

/**
 * @author chenruoyu
 *
 */
public class PageRank implements Serializable {

	private static final long serialVersionUID = 3662916104012783823L;
	public static final double DELTA = 0.85;// 摩擦系数、阻尼系数
	public static final double NON_CONVERGING_FACTOR = 0.01;
	public static final double DIFF = 0.001;
	public static final double ZERO = 0.0;
	public static final double ONE = 1.0;

	public int run(String dir, String output) {
		/**
		 * 初始化Spark环境
		 */
		SparkConf sparkConf = new SparkConf().setAppName("PageRankSpark" + System.currentTimeMillis());
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		/**
		 * 读取输入文件
		 */
		JavaRDD<String> raw = sc.textFile(dir + "/*");

		/**
		 * 数据预处理，形成邻接链表，同时统计页面总数
		 */
		JavaPairRDD<Long, Node> adjList = raw
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Long, Node>() {
					private static final long serialVersionUID = -3764092888326577981L;
					public Iterator<Tuple2<Long, Node>> call(Iterator<String> value) throws Exception {
						// value是如下的字符串组成的列表
						// 1,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
						// 字符串含义为srcId,PageRank,AdjacentList
						ArrayList<Tuple2<Long, Node>> graph = new ArrayList();
						while (value.hasNext()) {
							String v = value.next();
							if (v == null || "".equals(v))
								continue;
							String[] list = v.split(",");
							Long srcId = Long.parseLong(list[0]);
							Node node = null;
							if (list.length == 2) {
								node = new Node(srcId, null);
							} else {
								long[] destIds = new long[list.length - 2];
								for (int i = 2; i < list.length; i++) {
									destIds[i - 2] = Long.parseLong(list[i]);
								}
								node = new Node(srcId, destIds);
							}
							graph.add(new Tuple2<Long, Node>(srcId, node));
						}
						return graph.iterator();
					}
				});
		adjList.persist(StorageLevel.MEMORY_AND_DISK_SER());
		/*
		 * 获取页面总数， 
		 * 在整个计算执行过程中，这个值是常量，不需要重复计算
		 */
		long PAGES = adjList.count();
		System.out.println("总页面数:" + PAGES);
		long nonConvergingPages = PAGES;// 一开始，假定未收敛的页面个数等于页面总数
		/**
		 * 将所有页面的PR值初始化为1.0，形成初始PageRank RDD
		 */
		JavaPairRDD<Long, Double> pageRank = adjList.mapValues(new Function<Node, Double>() {
			private static final long serialVersionUID = 6419553615194608424L;
			public Double call(Node arg0) throws Exception {
				return ONE;
			}
		});
		/**
		 * 迭代计算PageRank，选定的结束条件是 未收敛页面数大于总页面数的NON_CONVERGING_FACTOR
		 */
		int counter = 0;
		final DoubleAccumulator PARTIAL_PR_ACCU = sc.sc().doubleAccumulator("PARTIAL_PR");
		final LongAccumulator NON_CONVERGING_PAGES_ACCU = sc.sc().longAccumulator("NON_CONVERGING_PAGES");
		while (nonConvergingPages >= PAGES * NON_CONVERGING_FACTOR) {
			System.out.println("开始第" + (counter++) + "轮迭代");
			System.out.println("未收敛页面个数:" + nonConvergingPages);
			PARTIAL_PR_ACCU.reset();
			NON_CONVERGING_PAGES_ACCU.reset();
			/**
			 * 迭代计算的第一步：计算由链接关系传递的部分PR值
			 */
			JavaPairRDD<Long, Double> partialPR = adjList
					.join(pageRank)
					.mapPartitionsToPair(
					new PairFlatMapFunction<Iterator<Tuple2<Long, Tuple2<Node, Double>>>, Long, Double>() {
						private static final long serialVersionUID = 6387806599947414237L;
						public Iterator<Tuple2<Long, Double>> call(Iterator<Tuple2<Long, Tuple2<Node, Double>>> nodes)
								throws Exception {
							List<Tuple2<Long, Double>> result = new ArrayList();
							while (nodes.hasNext()) {
								Tuple2<Long, Tuple2<Node, Double>> tuple = nodes.next();
								long srcId = tuple._1;// 源页面ID
								Node node = tuple._2._1;// 源页面的邻接表
								double srcPr = tuple._2._2;// 源页面的PageRank值
								/**
								 * 有两类特殊页面：
								 * 汇点（Sink），即没有对外链接的页面
								 * 源（Source），即没有链接指向它的页面
								 * 这两类页面都需要特殊处理
								 */
								result.add(new Tuple2<Long, Double>(srcId, ZERO));
								if (!node.isDanglingNode()) {
									// 非汇点页面
									double p = srcPr / node.getDestIds().length;
									for (Long destId : node.getDestIds()) {
										result.add(new Tuple2<Long, Double>(destId, p));
									}
								}
							}
							return result.iterator();
						}
					})
					.reduceByKey(new Function2<Double, Double, Double>() {
						private static final long serialVersionUID = -8910876946046811474L;
						public Double call(Double arg0, Double arg1) throws Exception {
							return arg0 + arg1;
						}
					});
			/**
			 * 计算汇点页面PR值之和=总页面PR值（即页面总数）-通过链接传递的页面PR值,
			 * 将汇点页面PR值之和除以页面总数，
			 * 得到汇点页面分配给每个页面的PR值
			 */
			partialPR.foreach(new VoidFunction<Tuple2<Long,Double>>() {
				private static final long serialVersionUID = -3169774123452324432L;
				public void call(Tuple2<Long, Double> arg0) throws Exception {
					PARTIAL_PR_ACCU.add(arg0._2);
				}
			});
			final double danglingPr = (PAGES - PARTIAL_PR_ACCU.value())/PAGES;
			System.out.println("汇点页面PR值之和：" + danglingPr*PAGES);
			/**
			 * 迭代计算的第二步：在部分PR值基础上， 
			 * 结合汇点页面传递的PR值，并考虑摩擦因子， 
			 * 算出完整PR值
			 */
			JavaPairRDD<Long, Double> fullPR = partialPR
					.mapValues(new Function<Double, Double>() {
						private static final long serialVersionUID = 4480176021165466325L;
						public Double call(Double partialPR) throws Exception {
							//页面的PR值=1-DELTA+DELTA*(partialPR+danglingPr)
							return (1 - DELTA + DELTA * (danglingPr + partialPR));
						}
					});
			/**
			 * 对比前后两轮的PageRank值，计算出未收敛的页面个数
			 */
			fullPR.join(pageRank).foreach(new VoidFunction<Tuple2<Long,Tuple2<Double,Double>>>() {
				private static final long serialVersionUID = -707275790932530868L;
				public void call(Tuple2<Long, Tuple2<Double, Double>> arg0) throws Exception {
					if(Math.abs(arg0._2._1-arg0._2._2)>DIFF){
						//前后两轮节点的PR值超过阈值，未收敛
						NON_CONVERGING_PAGES_ACCU.add(1);
					}
				}
			});
			nonConvergingPages = NON_CONVERGING_PAGES_ACCU.value();
			System.out.println("未收敛页面数：" + nonConvergingPages);
			pageRank = fullPR;
		}
		pageRank.saveAsTextFile(output);// 输出计算结果
		sc.close();
		return 0;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("缺少输入文件目录参数");
			System.exit(0);
		}
		PageRank task = new PageRank();
		String input = args[0];
		String output = "/out/" + System.currentTimeMillis();
		long start = System.currentTimeMillis();
		task.run(input, output);
		System.out.println("PageRank计算总用时:" + (System.currentTimeMillis() - start) + "ms");
	}
}