package com.nec.aim.uid.client.common;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMonitor {	
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testMonitor() {		
	      WatchService watcher;
	        try {
	            watcher = FileSystems.getDefault().newWatchService();

	            Watchable path = Paths.get("C:\\Users\\xia\\Desktop\\test");
	            path.register(watcher,StandardWatchEventKinds.ENTRY_CREATE,StandardWatchEventKinds.ENTRY_MODIFY);	
	        } catch (IOException e) {
	            e.printStackTrace();
	            return;
	        }
	        while (true) {
	            WatchKey watchKey;
	            try {
	                watchKey = watcher.take();
	            } catch (InterruptedException e) {
	                System.err.println(e.getMessage());
	                return;
	            }

	            for (WatchEvent<?> event : watchKey.pollEvents()) {
	                Kind<?> kind = event.kind();
	                Object context = event.context();
	                System.out.println("kind=" + kind + ", context=" + context);
	            }

	            if (!watchKey.reset()) {
	                System.out.println("WatchKey が無効になりました");
	                return;
	            }
	        }
	    }

}
