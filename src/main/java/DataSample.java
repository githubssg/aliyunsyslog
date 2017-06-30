

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.Histogram;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.QueriedLog;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.log.response.GetCursorResponse;
import com.aliyun.openservices.log.response.GetHistogramsResponse;
import com.aliyun.openservices.log.response.GetLogsResponse;
import com.aliyun.openservices.log.response.ListShardResponse;
import com.aliyun.openservices.log.response.ListTopicsResponse;

class ClientSample {
	private final String endPoint = "cn-beijing.log.aliyuncs.com";
	private final String akId = "LTAImJgjJ3F6p4qM";
	private final String ak = "jhv0w2Heddxh8mmil20PoY2CQZ2ip7";
	private final Client client = new Client(endPoint, akId, ak);
	private final String project = "ssm";
	private final String logStore = "ssm_logstore";
	private final String topic = "ssg";

	public ClientSample() {
	}

	public void GetCursor() {

		try {
			ListShardResponse shard_res = client.ListShard(project, logStore);
			for (Shard shard : shard_res.GetShards()) {
				int shard_id = shard.GetShardId();

				GetCursorResponse res;
				long fromTime = (int) (System.currentTimeMillis() / 1000.0 - 3600);
				res = client.GetCursor(project, logStore, shard_id, fromTime);
				System.out.println("Cursor:" + res.GetCursor());

				res = client.GetCursor(project, logStore, shard_id,
						CursorMode.BEGIN);
				System.out.println("Cursor:" + res.GetCursor());

				res = client.GetCursor(project, logStore, shard_id,
						CursorMode.END);
				System.out.println("shard_id:" + shard_id + " Cursor:"
						+ res.GetCursor());
			}
		} catch (LogException e) {
			e.printStackTrace();
		}

	}

	public void BatchGetLog() {
		try {
			ListShardResponse res = client.ListShard(project, logStore);
			for (Shard shard : res.GetShards()) {
				int shard_id = shard.GetShardId();
				GetCursorResponse cursorRes = client.GetCursor(project,
						logStore, shard_id, CursorMode.BEGIN);
				String cursor = cursorRes.GetCursor();
				for (int j = 0; j < 3; j++) {
					BatchGetLogResponse logDataRes = client.BatchGetLog(
							project, logStore, shard_id, 100, cursor);
					List<LogGroupData> logGroups = logDataRes.GetLogGroups();
					for (LogGroupData logGroup : logGroups) {
						System.out.println("Source:" + logGroup.GetSource());
						System.out.println("Topic:" + logGroup.GetTopic());
						for (LogItem log : logGroup.GetAllLogs()) {
							System.out.println("LogTime:" + log.GetTime());
							List<LogContent> contents = log.GetLogContents();
							for (LogContent content : contents) {
								System.out.println(content.GetKey() + ":"
										+ content.GetValue());
							}
						}
					}
					String next_cursor = logDataRes.GetNextCursor();
					System.out.print("The Next cursor:" + next_cursor);

					if (cursor.equals(next_cursor)) {
						break;
					}
					cursor = next_cursor;
				}
			}
		} catch (LogException e) {
			e.printStackTrace();
		}
	}

	public void ListShard() {
		try {
			ListShardResponse res = client.ListShard(project, logStore);
			for (Shard shard : res.GetShards()) {

				System.out.println("ShardId:" + shard.GetShardId());
			}
		} catch (LogException e) {
			e.printStackTrace();
		}
	}

	public void PutData() {
		int log_group_num = 10;
		/**
		 * 向SLS发送一个日志包，每个日志包中，有2行日志
		 */
		for (int i = 0; i < log_group_num; i++) {
			Vector<LogItem> logGroup = new Vector<LogItem>();
			LogItem logItem = new LogItem((int) (new Date().getTime() / 1000));
			logItem.PushBack("level", "info");
			logItem.PushBack("name", String.valueOf(i));
			logItem.PushBack("message", "it's a test message");

			logGroup.add(logItem);

			LogItem logItem2 = new LogItem((int) (new Date().getTime() / 1000));
			logItem2.PushBack("level", "error");
			logItem2.PushBack("name", String.valueOf(i));
			logItem2.PushBack("message", "it's a test message");
			logGroup.add(logItem2);

			try {
				client.PutLogs(project, logStore, topic, logGroup, "");
			} catch (LogException e) {
				System.out.println("error code :" + e.GetErrorCode());
				System.out.println("error message :" + e.GetErrorMessage());
				System.out.println("error requestId :" + e.GetRequestId());
			}

		}
	}

	public void ListLogStore() {
		try {
			/**
			 * 查询project中所有logStore的名字
			 */
			ArrayList<String> logStores = client.ListLogStores(project, 0, 500,
					"").GetLogStores();
			System.out.println("ListLogs:" + logStores.toString() + "\n");
		} catch (LogException e) {
			System.out.print(e.getCause());
			System.out.println("error code :" + e.GetErrorCode());
			System.out.println("error message :" + e.GetErrorMessage());
			System.out.println("error requestId :" + e.GetRequestId());
		}
	}

	public void ListTopic() {
		/**
		 * 查询logstore中的topic的名字
		 */
		try {
			String token = "";
			ListTopicsResponse listTopicResponse = client.ListTopics(project,
					logStore, token, 10);
			ArrayList<String> topics = listTopicResponse.GetTopics();
			System.out.println("ListTopics:" + topics.toString());
			System.out.println("NextTopic:" + listTopicResponse.GetNextToken()
					+ "\n");
		} catch (LogException e) {
			System.out.println("error code :" + e.GetErrorCode());
			System.out.println("error message :" + e.GetErrorMessage());
			System.out.println("error requestId :" + e.GetRequestId());
		}
	}

	public void QueryLog() {
		/**
		 * 查询logstore的histogram信息
		 */
		try {
			String query = "";
			int from = (int) (new Date().getTime() / 1000 - 10000);
			int to = (int) (new Date().getTime() / 1000 + 10);
			GetHistogramsResponse histogramsResponse = client.GetHistograms(
					project, logStore, from, to, topic, query);
			System.out.println("histogram result: "
					+ histogramsResponse.GetTotalCount());
			System.out.println("is_completed : "
					+ histogramsResponse.IsCompleted());
			for (Histogram histogram : histogramsResponse.GetHistograms()) {
				System.out.println("beginTime:" + histogram.mFromTime
						+ " endTime:" + histogram.mToTime + " logCount:"
						+ histogram.mCount + " is_completed:"
						+ histogram.mIsCompleted);
			}
			System.out.println("");
		} catch (LogException e) {
			System.out.println("error code :" + e.GetErrorCode());
			System.out.println("error message :" + e.GetErrorMessage());
			System.out.println("error requestId :" + e.GetRequestId());
		}

		/**
		 * 查询logstore的内容
		 */
		try {
			String query = "error";
			int from = (int) (new Date().getTime() / 1000 - 10000);
			int to = (int) (new Date().getTime() / 1000 + 10);
			GetLogsResponse logsResponse = client.GetLogs(project, logStore,
					from, to, topic, query, 10, 0, false);
			System.out.println("Returned log data count:"
					+ logsResponse.GetCount());
			for (QueriedLog log : logsResponse.GetLogs()) {
				System.out.println("source : " + log.GetSource());
				LogItem item = log.GetLogItem();
				System.out.println("time : " + item.mLogTime);
				for (LogContent content : item.mContents) {
					System.out.println(content.mKey + ":" + content.mValue);
				}
			}
		} catch (LogException e) {
			System.out.println("error code :" + e.GetErrorCode());
			System.out.println("error message :" + e.GetErrorMessage());
		}
	}
}

public class DataSample {
	public static void main(String[] args) {
		System.out.print("asd");
		// ------------------------Data API------------------------
		ClientSample sample = new ClientSample();
		sample.ListLogStore();
		sample.PutData();
		sample.ListShard();

		sample.GetCursor();
		sample.BatchGetLog();
		try {
			Thread.sleep(30 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		sample.ListTopic();
		sample.QueryLog();
	}
}