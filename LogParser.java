package com.codegym.task.task39.task3913;

import com.codegym.task.task39.task3913.query.*;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class LogParser implements IPQuery, UserQuery, DateQuery, EventQuery, QLQuery {

    private List<Path> logFiles = new ArrayList<>();
    private List<String> logStrings = new ArrayList<>();
    private List<Log> logObjects = new ArrayList<>();

    public LogParser(Path logDir) {

        try {
            Files.walkFileTree(logDir, new SimpleFileVisitor<Path>(){
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs){

                    String fileName = file.getFileName().toString();
                    if (fileName.endsWith(".log")) logFiles.add(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logFilesToOneList();
        logStringsToLogObjects();
    }

    private void logFilesToOneList(){

        List<String> temp;

        for (Path logFile : logFiles) {
            try {
                temp = Files.readAllLines(logFile);
                logStrings.addAll(temp);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void logStringsToLogObjects() {

        String ip;
        String name;
        Date date;
        Event event = null;
        Integer taskNumber = null;
        Status status = null;

        for (String logString : logStrings) {
            String[] elements = logString.split("\t");
            ip = elements[0];
            name = elements[1];

            SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
            try {
                date = sdf.parse(elements[2]);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            String[] eventHasNumber = elements[3].split(" ");
            for (Event values : Event.values()) {
                if (values.toString().equals(eventHasNumber[0])) event = values;
            }
            if (eventHasNumber.length == 1) taskNumber = null;
            if (eventHasNumber.length == 2) taskNumber = Integer.parseInt(eventHasNumber[1]);

            for (Status values : Status.values()) {
                if (values.toString().equals(elements[4])) status = values;
            }
            logObjects.add(new Log(ip, name, date, event, taskNumber, status));
            }
    }

    private List<Log> getLogsWithGoodDates(Date after, Date before){
        List<Log> result = new ArrayList<>();
        if (after == null && before == null) return logObjects;

        if (after == null) {
            for (Log logObject : logObjects) {
                if (logObject.date.before(before)) result.add(logObject);
            }
            return result;
        }

        if (before == null) {
            for (Log logObject : logObjects) {
                if (logObject.date.after(after)) result.add(logObject);
            }
            return result;
        }

        for (Log logObject : logObjects) {
            if (logObject.date.after(after) && logObject.date.before(before)) result.add(logObject);
        }

        return result;
    }

    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {

        List<Log> correctDates = getLogsWithGoodDates(after, before);

        TreeSet<String> uniqueIps = new TreeSet<>();

        for (Log logObject : correctDates) {
            uniqueIps.add(logObject.ip);
        }

        return uniqueIps.size();
    }

    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {

        List<Log> correctDates = getLogsWithGoodDates(after, before);

        TreeSet<String> uniqueIps = new TreeSet<>();

        for (Log logObject : correctDates) {
            uniqueIps.add(logObject.ip);
        }

        return uniqueIps;
    }

    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {

        List<Log> correctDates = getLogsWithGoodDates(after, before);

        Set<String> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.name.equals(user)) result.add(logObject.ip);
        }

        return result;
    }

    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {

        List<Log> correctDates = getLogsWithGoodDates(after, before);
        Set<String> result = new HashSet<>();
        if (event == null) {
            for (Log correctDate : correctDates) {
                result.add(correctDate.ip);
            }
            return result;
        }

        for (Log logObject : correctDates) {
            if (logObject.event.equals(event)) result.add(logObject.ip);
        }

        return result;
    }

    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {

        List<Log> correctDates = getLogsWithGoodDates(after, before);

        Set<String> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.status.equals(status)) result.add(logObject.ip);
        }

        return result;

    }

    @Override
    public Set<String> getAllUsers() {
        TreeSet<String> result = new TreeSet<>();

        for (Log logObject : logObjects) {
            result.add(logObject.name);
        }

        return result;
    }

    @Override
    public int getNumberOfUsers(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        TreeSet<String> result = new TreeSet<>();

        for (Log logObject : correctDates) {
            result.add(logObject.name);
        }

        return result.size();
    }

    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {

        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<Event> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.name.equals(user)) result.add(logObject.event);
        }

        return result.size();
    }

    @Override
    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<String> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.ip.equals(ip)) result.add(logObject.name);
        }

        return result;
    }

    @Override
    public Set<String> getUsersWhoHaveLoggedIn(Date after, Date before) {

        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<String> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.LOGIN)) result.add(logObject.name);
        }

        return result;
    }

    @Override
    public Set<String> getUsersWhoHaveDownloadedPlugin(Date after, Date before) {

        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<String> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.DOWNLOAD_PLUGIN) && logObject.status.equals(Status.OK)) result.add(logObject.name);
        }

        return result;
    }

    @Override
    public Set<String> getUsersWhoHaveSentMessages(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<String> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.SEND_MESSAGE) && logObject.status.equals(Status.OK)) result.add(logObject.name);
        }

        return result;
    }

    @Override
    public Set<String> getUsersWhoHaveAttemptedTasks(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<String> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.ATTEMPT_TASK)) result.add(logObject.name);
        }

        return result;
    }

    @Override
    public Set<String> getUsersWhoHaveAttemptedTasks(Date after, Date before, int task) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<String> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.ATTEMPT_TASK) && logObject.taskNumber == task)
                result.add(logObject.name);
        }

        return result;
    }

    @Override
    public Set<String> getUsersWhoHaveCompletedTasks(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<String> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.COMPLETE_TASK))
                result.add(logObject.name);
        }

        return result;
    }

    @Override
    public Set<String> getUsersWhoHaveCompletedTasks(Date after, Date before, int task) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<String> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.COMPLETE_TASK) && logObject.taskNumber == task)
                result.add(logObject.name);
        }

        return result;
    }

    @Override
    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<Date> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.name.equals(user) && logObject.event.equals(event)) result.add(logObject.date);
        }

        return result;
    }

    @Override
    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<Date> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.status.equals(Status.FAILED)) result.add(logObject.date);
        }

        return result;
    }

    @Override
    public Set<Date> getDatesWhenErrorOccurred(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<Date> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.status.equals(Status.ERROR)) result.add(logObject.date);
        }

        return result;
    }

    @Override
    public Date getDateWhenUserLoggedInFirstTime(String user, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        TreeSet<Date> loggingDates = new TreeSet<>();

        for (Log logObject : correctDates) {
            if (logObject.name.equals(user) && logObject.event.equals(Event.LOGIN)) loggingDates.add(logObject.date);
        }

        if (loggingDates.size() == 0) return null;

        return loggingDates.first();
    }

    @Override
    public Date getDateWhenUserAttemptedTask(String user, int task, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        TreeSet<Date> loggingDates = new TreeSet<>();

        for (Log logObject : correctDates) {
            if (logObject.name.equals(user) && logObject.event.equals(Event.ATTEMPT_TASK) && logObject.taskNumber == task)
                loggingDates.add(logObject.date);
        }

        if (loggingDates.size() == 0) return null;

        return loggingDates.first();
    }

    @Override
    public Date getDateWhenUserCompletedTask(String user, int task, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        TreeSet<Date> loggingDates = new TreeSet<>();

        for (Log logObject : correctDates) {
            if (logObject.name.equals(user) && logObject.event.equals(Event.COMPLETE_TASK) && logObject.taskNumber == task)
                loggingDates.add(logObject.date);
        }

        if (loggingDates.size() == 0) return null;

        return loggingDates.first();
    }

    @Override
    public Set<Date> getDatesWhenUserSentMessages(String user, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        TreeSet<Date> loggingDates = new TreeSet<>();

        for (Log logObject : correctDates) {
            if (logObject.name.equals(user) && logObject.event.equals(Event.SEND_MESSAGE)) loggingDates.add(logObject.date);
        }

        return loggingDates;
    }

    @Override
    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        TreeSet<Date> loggingDates = new TreeSet<>();

        for (Log logObject : correctDates) {
            if (logObject.name.equals(user) && logObject.event.equals(Event.DOWNLOAD_PLUGIN)) loggingDates.add(logObject.date);
        }

        return loggingDates;
    }

    @Override
    public int getNumberOfEvents(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        HashSet<Event> result = new HashSet<>();

        for (Log logObject : correctDates) {
            result.add(logObject.event);
        }
        return result.size();
    }

    @Override
    public Set<Event> getAllEvents(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        Set<Event> result = new HashSet<>();

        for (Log logObject : correctDates) {
            result.add(logObject.event);
        }
        return result;
    }

    @Override
    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        Set<Event> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.ip.equals(ip)) result.add(logObject.event);
        }
        return result;
    }

    @Override
    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        Set<Event> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.name.equals(user)) result.add(logObject.event);
        }
        return result;
    }

    @Override
    public Set<Event> getFailedEvents(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        Set<Event> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.status.equals(Status.FAILED)) result.add(logObject.event);
        }
        return result;
    }

    @Override
    public Set<Event> getErrorEvents(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        Set<Event> result = new HashSet<>();

        for (Log logObject : correctDates) {
            if (logObject.status.equals(Status.ERROR)) result.add(logObject.event);
        }
        return result;
    }

    @Override
    public int getNumberOfAttemptsToCompleteTask(int task, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        List<Event> result = new ArrayList<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.ATTEMPT_TASK) && logObject.taskNumber == task) result.add(logObject.event);
        }
        return result.size();
    }

    @Override
    public int getNumberOfSuccessfulAttemptsToCompleteTask(int task, Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);

        List<Event> result = new ArrayList<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.COMPLETE_TASK) && logObject.taskNumber == task)
                result.add(logObject.event);
        }
        return result.size();
    }

    @Override
    public Map<Integer, Integer> getAllAttemptedTasksAndNumberOfAttempts(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);
        TreeSet<Integer> tempSet = new TreeSet<>();
        TreeMap<Integer, Integer> result = new TreeMap<>();
        List<Integer> tempFreq = new ArrayList<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.ATTEMPT_TASK)){
                if (!tempSet.contains(logObject.taskNumber)) {
                    tempSet.add(logObject.taskNumber);
                }
                tempFreq.add(logObject.taskNumber);
            }
        }

        for (Integer integer : tempSet) {
            int key = integer;
            int frequency = Collections.frequency(tempFreq, key);
            result.put(key, frequency);
        }

        return result;
    }

    @Override
    public Map<Integer, Integer> getAllCompletedTasksAndNumberOfCompletions(Date after, Date before) {
        List<Log> correctDates = getLogsWithGoodDates(after, before);
        TreeSet<Integer> tempSet = new TreeSet<>();
        TreeMap<Integer, Integer> result = new TreeMap<>();
        List<Integer> tempFreq = new ArrayList<>();

        for (Log logObject : correctDates) {
            if (logObject.event.equals(Event.COMPLETE_TASK)){
                if (!tempSet.contains(logObject.taskNumber)) {
                    tempSet.add(logObject.taskNumber);
                }
                tempFreq.add(logObject.taskNumber);
            }
        }

        for (Integer integer : tempSet) {
            int key = integer;
            int frequency = Collections.frequency(tempFreq, key);
            result.put(key, frequency);
        }

        return result;
    }

    @Override
    public Set<Object> execute(String query) {

        Set<Date> dates = new HashSet<>();
        for (Log logObject : logObjects) {
            dates.add(logObject.date);
        }

        Set<Status> statuses = new HashSet<>();
        for (Log logObject : logObjects) {
            statuses.add(logObject.status);
        }

        switch (query){
            case "get ip":
                return getUniqueIPs(null, null).stream().collect(Collectors.toSet());
            case "get user":
                return getAllUsers().stream().collect(Collectors.toSet());
            case "get date":
                return dates.stream().collect(Collectors.toSet());
            case "get event":
                return getAllEvents(null, null).stream().collect(Collectors.toSet());
            case "get status":
                return statuses.stream().collect(Collectors.toSet());
            default:
                return queryParser(query).stream().collect(Collectors.toSet());
        }
    }

    private Set<Object> queryParser(String query){

        String[] twoSides = query.split("=");
        String[] firstSide = twoSides[0].split(" ");
        String field1 = firstSide[1];
        String field2 = firstSide[3];
        String[] values = twoSides[1].trim().replace("\"", "").split(" ");
//        String value1 = twoSides[1].substring(2, twoSides[1].length() - 1);
        String value1 = getValueFromString(twoSides[1]);
        boolean hasDateRange = false;

        Date date = null;
        Date[] beforeAfter = null;
        Date dateFrom = null;
        Date dateTo = null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

        if (twoSides[1].contains("between")){
            beforeAfter = getDatesBeforeAfter(twoSides[1]);
            dateFrom = beforeAfter[0];
            dateTo = beforeAfter[1];
            hasDateRange = true;
        }

        if (field2.equals("date")){
            try {
                date = simpleDateFormat.parse(value1);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

//        System.out.println(value1);

        Set<Log> tempLogObjects = new HashSet<>();

        for (Log logObject : logObjects) {

            switch (field2){
                case "user":
                    if (logObject.name.equals(value1)) tempLogObjects.add(logObject);
                    break;
                case "ip":
                    if (logObject.ip.equals(value1)) tempLogObjects.add(logObject);
                    break;
                case "date":
                    if (logObject.date.equals(date)) tempLogObjects.add(logObject);
                    break;
                case "status":
                    if (logObject.status.toString().equals(value1)) tempLogObjects.add(logObject);
                    break;
                case "event":
                    if (logObject.event.toString().equals(value1)) tempLogObjects.add(logObject);
                    break;
            }
        }

        if (hasDateRange){
            tempLogObjects = dateRangeFilter(tempLogObjects, dateFrom, dateTo);
        }

        Set<Object> result = new HashSet<>();

        for (Log tempLogObject : tempLogObjects) {
            switch (field1){
                case "user":
                    result.add(tempLogObject.name);
                    break;
                case "ip":
                    result.add(tempLogObject.ip);
                    break;
                case "date":
                    result.add(tempLogObject.date);
                    break;
                case "status":
                    result.add(tempLogObject.status);;
                    break;
                case "event":
                    result.add(tempLogObject.event);;
                    break;
            }
        }

        return result;
    }

    private Date[] getDatesBeforeAfter (String value){
        String[] elements = value.trim().replace("\"", "").split(" ");
        int length = elements.length;
        Date[] result = new Date[2];
        StringBuilder dateFrom = new StringBuilder().append(elements[length - 5]).append(" ").append(elements[length - 4]);
        StringBuilder dateTo = new StringBuilder().append(elements[length - 2]).append(" ").append(elements[length - 1]);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        try {
            result[0] = simpleDateFormat.parse(dateFrom.toString());
            result[1] = simpleDateFormat.parse(dateTo.toString());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    private Set<Log> dateRangeFilter(Set<Log> toFilter, Date after, Date before){
        Set<Log> result = new HashSet<>();

        for (Log logObject : toFilter) {
            if (logObject.date.after(after) && logObject.date.before(before)) result.add(logObject);
        }

        return result;
    }

    private String getValueFromString(String s){

        StringBuilder stringBuilder = new StringBuilder();
        String[] elements = s.trim().replace("\"", "").split(" ");

        for (String string : elements) {
            if (!string.equals("and")) {
                stringBuilder.append(string).append(" ");
            }
            else break;
        }

        return stringBuilder.toString().trim();
    }

    public class Log{
        String ip;
        String name;
        Date date;
        Event event;
        Integer taskNumber;
        Status status;

        public Log(String ip, String name, Date date, Event event, Integer taskNumber, Status status) {
            this.ip = ip;
            this.name = name;
            this.date = date;
            this.event = event;
            this.taskNumber = taskNumber;
            this.status = status;
        }
    }
}