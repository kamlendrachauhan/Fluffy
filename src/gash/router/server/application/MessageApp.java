/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.server.application;

import java.io.File;

/**
 * @author gash1
 * 
 */
public class MessageApp {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("usage: server <Config File> <Global Config File> <Is Monitering>");
			System.exit(1);
		}

		File cf = new File(args[0]);
		File globalCf = new File(args[1]);
		boolean isMonitering = Boolean.parseBoolean(args[2]);
		try {
			MessageServer svr = new MessageServer(cf, globalCf, isMonitering);
			svr.startServer();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("server closing");
		}
	}
}
