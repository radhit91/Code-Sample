/*** Implementation of a Broker Cache Management Module ***/

try {
    // Load MySQL driver
    Class.forName("com.mysql.jdbc.Driver");
    // Setup connection with the MYSQL DB
    connect = DriverManager.getConnection("jdbc:mysql://localhost/datab?"+ "user=root&password=radhit");
    // Remove MySQL tables from previous run 
    preparedStatement = connect.prepareStatement("truncate table food_pss");
    preparedStatement.executeUpdate();
    preparedStatement = connect.prepareStatement("truncate table sub_pss");
    preparedStatement.executeUpdate();
                
    while (true){
	   //Poll subscription log for new subscriptions
           ConsumerRecords<String, String> sub_records = sub_poller.poll(1);
	   //Update subscription table in MySQL DB
           for (ConsumerRecord<String, String> record : sub_records){
			String[] token = record.value().split("-");
			if(token[1].equals("1")){
				//add to list
				System.out.println("Added " + token[0]);
                		addToList(items, token[0], record.key());
				addToList(items_r, record.key(), token[0]);
				preparedStatement = connect.prepareStatement("insert into  datab.sub_pss values (default, ?, ?)");
	            		preparedStatement.setString(1, record.key());
	            		preparedStatement.setString(2, token[0]);
    	        		preparedStatement.executeUpdate();
			}
			else{
				//delete from list
				System.out.println("Deleted " + token[0]);
				deleteFromList(items, token[0], record.key());
				deleteFromList(items_r, record.key(), token[0]);
				preparedStatement = connect.prepareStatement("delete from  datab.sub_pss where subid='" + record.key() +"' and value='" + token[0] +"'");
    	        		preparedStatement.executeUpdate();
			}
           }

     //Poll registration log for new publishers
     ConsumerRecords<String, String> reg_records = reg_poller.poll(1);
     //Inform consumers about new publishers & send acknowledgments back to new publishers
     for (ConsumerRecord<String, String> record : reg_records){
	   System.out.println(getFromList(items_r, record.key()));
	   if(record.value().equals("1") && getFromList(items_r, record.key()).isEmpty()){
	   		System.out.println("New Publisher Add Request: " + record.key());
	   		for(String s : items_r.keySet()){
				producer.send(new ProducerRecord<String, String>("consumer"+s, "#1-" + record.key(), record.value()));
           		}
	   		producer.send(new ProducerRecord<String, String>("ack_log", record.key(), record.value()+ "-1"));
     	   }
	   else if(record.value().equals("0") && !getFromList(items_r, record.key()).isEmpty()){
			System.out.println("Old Publisher Delete Request: " + record.key());
			producer.send(new ProducerRecord<String, String>("ack_log", record.key(), record.value()+ "-1"));
	   }
	   else{
			System.out.println("Invalid Request: " + record.key() + " " + record.value());
			producer.send(new ProducerRecord<String, String>("ack_log", record.key(), record.value()+ "-0"));
	   }
    }

    //Poll publication log for new publications
    ConsumerRecords<String, String> pub_records = pub_poller.poll(1);
    //For each incoming publication, check if it is to be cached and send it to its destination appropriately
    for (ConsumerRecord<String, String> record : pub_records){
	   pub_seq_no++;
	   String[] token = record.value().split(";");
	   ArrayList<String> list_of_sub = getFromList(items, token[0]);
	   no_of_subs = list_of_sub.size();
	   isCached = cache_function.update(no_of_subs, Integer.parseInt(args[0]));

	   for(String s : list_of_sub){
	 		seq_no = lastSeqNo.get(Integer.parseInt(s)-1); 
			lastSeqNo.set(Integer.parseInt(s)-1, seq_no + 1);
	   }

	   if(isCached){
           	        cache_hits++;
			duplication_sum = duplication_sum + no_of_subs;  
                	for(String s : list_of_sub){
				producer.send(new ProducerRecord<String, String>("consumer"+s, lastSeqNo.get(Integer.parseInt(s)-1).toString(), record.value()));
               		}
            }
	    else{
			for(String s : list_of_sub){
				preparedStatement = connect.prepareStatement("insert into  datab.food_pss values (default, ?, ?, ?)");
	            		preparedStatement.setString(1, s);
	            		preparedStatement.setString(2, lastSeqNo.get(Integer.parseInt(s)-1).toString());
    	        		preparedStatement.setString(3, record.value());
    	        		preparedStatement.executeUpdate();
			}
	    }
    }

    //Print caching statistics & fill consumer logs
    if(pub_seq_no == Integer.parseInt(args[1]) && flag == 0){
	    System.out.println("Cache Hit Rate: " + cache_hits/pub_seq_no);
	    if(cache_hits != 0)
		System.out.println("Duplication Factor: " + duplication_sum/cache_hits);
	    flag = 1;
	    for(int i = 0; i < lastSeqNo.size(); i++){
		int dummy_seq_no = lastSeqNo.get(i) + 1;
		producer.send(new ProducerRecord<String, String>("consumer"+consumerID.get(i), ""+dummy_seq_no, ""));
	    }
    }
           
    //Poll database log for missing consumer blocks(to be fetched from database) 
    ConsumerRecords<String, String> database_records = database_poller.poll(1);
    //Retrieve missing blocks from database & pass it on to relevant consumers
    for (ConsumerRecord<String, String> record : database_records){
	     //Fetch from database and write to corresponding cache log
	     statement = connect.createStatement();
             // Result set get the result of the SQL query
             resultSet = statement.executeQuery("select value from food_pss where subid='" + record.key() +"' and seqno='" + record.value() +"'");
             String value = writeResultSet(resultSet);
	     producer.send(new ProducerRecord<String, String>("consumer"+record.key(), record.value(), value));
    }
    }
}catch (Exception e) {
     System.out.println(e);
     System.out.println("Error. Terminating....");
     System.out.println("Terminated.");
} 
