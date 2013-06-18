package com.trifork;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPRiakClientFactory;


public class Main {

	private int maxThreads = 100;
	private ThreadPoolExecutor deleteThreadPoolExecutor = new ThreadPoolExecutor(1, maxThreads, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
	private ThreadPoolExecutor listKeysThreadPoolExecutor = new ThreadPoolExecutor(1, 10, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(100), new ThreadPoolExecutor.CallerRunsPolicy());
	
	public static void main(String[] args) throws Exception {
		new Main().run();
	}

	private void run() throws Exception {
		Properties properties = new Properties();
		properties.load(new FileInputStream("bucketdeleter.properties"));
		String riakHost = properties.getProperty("riak.host");
		int riakPort = Integer.parseInt(properties.getProperty("riak.port"));
		
		final RawClient riakClient = createRiakClient(riakHost, riakPort);
		
		Collection<String> buckets = readBuckets();
		System.out.println("The program will delete all keys in the following buckets " + buckets);
		System.out.println("Press y to continue");
		Scanner scan = new Scanner (System.in);
		String input = scan.next();
		scan.close();
		if (!"y".equals(input)) {
			System.out.println("terminating");
			System.exit(0);
		}
		
		for (final String b : buckets) {
			listKeysThreadPoolExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						deleteKeysInBucket(b, riakClient);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}
		listKeysThreadPoolExecutor.shutdown();
		listKeysThreadPoolExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
		deleteThreadPoolExecutor.shutdown();
	}
	
	private Collection<String> readBuckets() throws Exception {
		Collection<String> res = new ArrayList<>();
		FileReader fr = new FileReader("buckets.txt");
		BufferedReader reader = new BufferedReader(fr);
		String line;
		while ((line = reader.readLine()) != null) {
			String bucket = line.trim();
			if (!bucket.isEmpty()) {
				res.add(line);
			}
		}
		reader.close();
		fr.close();
		return res;
	}

	private RawClient createRiakClient(String riakHost, int riakPort) throws RiakException {
        HTTPClientConfig.Builder cb = new HTTPClientConfig.Builder()
                .withHost(riakHost)
                .withPort(riakPort)
                .withMaxConnections(maxThreads + 1)
                .withTimeout(30000);
        return HTTPRiakClientFactory.getInstance().newClient(cb.build());
    }
	
	private void deleteKeysInBucket(final String bucket, final RawClient riakClient) throws Exception {
		System.out.println("reading keys in " + bucket);
		final AtomicInteger size = new AtomicInteger();
		Iterable<String> keys = riakClient.listKeys(bucket);
		for (final String k : keys) {
			deleteThreadPoolExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						riakClient.delete(bucket, k);
						int value = size.incrementAndGet();
						if (value % 10000 == 0) {
							System.out.format("deleted %d keys in %s\n", value, bucket);
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}
		deleteThreadPoolExecutor.execute(new Runnable() {
			@Override
			public void run() {
				System.out.println("done deleting keys in bucket " + bucket);
			}
		});
		System.out.format("Done reading keys for bucket %s. Read %d keys\n", bucket, size.get());
	}
	
	
}
