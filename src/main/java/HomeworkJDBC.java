
import java.sql.*;
import java.util.List;
import java.util.Map;

public class HomeworkJDBC {

    private static final String DATA_BASE_URL = "jdbc:mysql://localhost:3306/evdokia";
    private static final String DATA_BASE_USER = "evdokia";
    private static final String DATA_BASE_PASSWORD = "evdokia";

    private static final String CREATE_TABLE_CURATOR_SQL =
            "CREATE TABLE IF NOT EXISTS Curator" +
                    "(id int AUTO_INCREMENT PRIMARY KEY," +
                    "FIO VARCHAR (50))";

    private static final String CREATE_TABLE_GROUP_SQL =
            "CREATE TABLE IF NOT EXISTS Grooup" +
                    "(id int AUTO_INCREMENT PRIMARY KEY," +
                    "name VARCHAR (50)," +
                    "id_curator int," +
                    "FOREIGN KEY (id_curator) REFERENCES Curator(id))";

    private static final String CREATE_TABLE_STUDENT_SQL =
            "CREATE TABLE IF NOT EXISTS Student" +
                    "(id int AUTO_INCREMENT PRIMARY KEY," +
                    "fio VARCHAR (50)," +
                    "sex VARCHAR (1)," +
                    "id_grooup int," +
                    "FOREIGN KEY (id_grooup) REFERENCES Grooup(id))";

    private static final String INSERT_INTO_CURATOR =
            "INSERT INTO Curator(FIO) VALUES(?)";

    public static final List<String> CURATOR_FIO_COLLECTION = List.of(
            "Иванов Иван Иванович",
            "Петров Петр Петрович",
            "Александров Александр Александрович",
            "Михайлов Михаил Михайлович");

    private static final String INSERT_INTO_GROUP =
            "INSERT INTO Grooup(name, id_curator) VALUES(?, ?)";

    private static final List<String> GROUP_NAME_COLLECTION = List.of(
            "Первая",
            "Вторая",
            "Третья");


    private static final String INSERT_INTO_STUDENT =
            "INSERT INTO Student(fio, sex, id_grooup) VALUES(?, ?, ?)";

    private static final Map<String, String> STUDENT_FIO_COLLECTION = Map.ofEntries(
            Map.entry("Васильев Александр Иванович", "М"),
            Map.entry("Антонова Мария Петровна", "Ж"),
            Map.entry("Шубин Корней Богданович", "М"),
            Map.entry("Романова Нина Леонидовна", "Ж"),
            Map.entry("Максимов Виктор Андреевич", "М"),
            Map.entry("Яковлева Вера Ивановна", "Ж"),
            Map.entry("Савельев Иван Сергеевич", "М"),
            Map.entry("Белов Станислав Проклович", "М"),
            Map.entry("Тихонов Павел Борисович", "М"),
            Map.entry("Денисов Евгений Владимирович", "М"),
            Map.entry("Абрамов Матвей Проклович", "М"),
            Map.entry("Доронина Нина Олеговна", "Ж"),
            Map.entry("Жуков Борис Денисович", "М"),
            Map.entry("Быкова Ксения Семёновна", "Ж"),
            Map.entry("Васильева Марина Георгиевна", "Ж"));

    private static final String DROP_TABLES_IF_EXIST = "DROP TABLE IF EXISTS Curator, Grooup, Student CASCADE";

    private static final String CREATE_READ_STUDENT_INFO =
            "SELECT S.fio, S.sex, G.name, C.fio " +
                    "FROM Student S " +
                    "join Grooup G on G.id = S.id_grooup " +
                    "join Curator C on C.id = G.id_curator";

    private static final String READ_STUDENT_COUNT =
            "SELECT count(distinct id) FROM Student";

    private static final String READ_FEMALE_STUDENT_COUNT =
            "SELECT count(distinct id) FROM Student where sex = 'Ж'";

    private static final String CHANGE_CURATOR =
            "UPDATE Grooup SET id_curator = 4 where id = 3";

    private static final String READ_GROUP_INFO =
            "SELECT G.name, C.FIO " +
                    "from Grooup G " +
                    "join Curator C on C.id = G.id_curator";

    private static final String READ_STUDENT_BY_GROUP_NAME =
            "SELECT S.fio " +
                    "FROM Student S " +
                    "where S.id_grooup = ( " +
                    "    SELECT id FROM Grooup G " +
                    "    where G.name = ? " +
                    ")";

    private static final List<String> groupNames = List.of(
            "Первая",
            "Вторая",
            "Третья"
    );


    public static void main(String[] args) throws SQLException {
        try (Connection connection = DriverManager.getConnection(DATA_BASE_URL, DATA_BASE_USER, DATA_BASE_PASSWORD)) {
            dropTablesIfExist(connection);
            createTableCurator(connection);
            createTableGrooup(connection);
            createTableStudent(connection);

            for (String fio : CURATOR_FIO_COLLECTION) {
                insertDataIntoCuratorTable(connection, fio);
            }

            int curatorId = 1;

            for (String groupName : GROUP_NAME_COLLECTION) {
                insertDataIntoGroupTable(connection, groupName, curatorId);
                curatorId++;
            }

            int groupId = 1;

            for (String studentFio : STUDENT_FIO_COLLECTION.keySet()) {
                if (groupId > 3) {
                    groupId = 1;
                }
                insertDataIntoStudentTable(connection, studentFio, STUDENT_FIO_COLLECTION.get(studentFio), groupId);
                groupId++;
            }

            readStudentInfo(connection);
            readStudentCount(connection);
            readFemaleStudentCount(connection);

            System.out.println("Информация о группах ДО обновления Куратора");
            readGroupInfo(connection);
            changeCurator(connection);
            System.out.println("Информация о группах ПОСЛЕ обновления Куратора");
            readGroupInfo(connection);

            for (String groupName : groupNames) {
                System.out.println("Информация о студентах из группы: " + groupName);
                readStudentInfoByGroupName(connection, groupName);
            }
        }

    }

    private static void readStudentInfoByGroupName(Connection connection, String groupName) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(READ_STUDENT_BY_GROUP_NAME)) {
            preparedStatement.setString(1, groupName);
            ResultSet resultSet = preparedStatement.executeQuery();
            printData(resultSet);
        }
    }

    private static void changeCurator(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(CHANGE_CURATOR);
        }
    }

    private static void dropTablesIfExist(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(DROP_TABLES_IF_EXIST);
        }
    }

    private static void createTableCurator(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE_CURATOR_SQL);
        }
    }

    private static void createTableGrooup(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE_GROUP_SQL);
        }
    }

    private static void createTableStudent(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE_STUDENT_SQL);
        }
    }

    private static void insertDataIntoCuratorTable(Connection connection, String fio) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_CURATOR)) {
            statement.setString(1, fio);
            int insertedRowsNumber = statement.executeUpdate();
            System.out.println("Inserted rows number: " + insertedRowsNumber);
        }
    }

    private static void insertDataIntoGroupTable(Connection connection, String name, int curatorId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_GROUP)) {
            statement.setString(1, name);
            statement.setString(2, String.valueOf(curatorId));
            int insertedRowsNumber = statement.executeUpdate();
            System.out.println("Inserted rows number: " + insertedRowsNumber);
        }
    }

    private static void insertDataIntoStudentTable(Connection connection, String studentFio, String sex, int groupId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_STUDENT)) {
            statement.setString(1, studentFio);
            statement.setString(2, sex);
            statement.setString(3, String.valueOf(groupId));
            int insertedRowsNumber = statement.executeUpdate();
            System.out.println("Inserted rows number: " + insertedRowsNumber);
        }
    }

    private static void readStudentInfo(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(CREATE_READ_STUDENT_INFO);
            printData(resultSet);
        }
    }

    private static void printData(ResultSet data) throws SQLException {
        while (data.next()) {
            for (int i = 1; i <= data.getMetaData().getColumnCount(); i++) {
                String columnName = data.getMetaData().getColumnName(i);
                String columnValue = data.getString(i);
                System.out.println(columnName + ": " + columnValue);
            }
            System.out.println("________________________________________");
        }
    }

    private static void readStudentCount(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(READ_STUDENT_COUNT);
            resultSet.next();
            int studentCount = resultSet.getInt(1);
            System.out.println(String.format("Количество студентов: %s", studentCount));
        }
    }

    private static void readFemaleStudentCount(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(READ_FEMALE_STUDENT_COUNT);
            resultSet.next();
            int studentCount = resultSet.getInt(1);
            System.out.println(String.format("Количество студенток: %s", studentCount));
        }
    }

    private static void readGroupInfo(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(READ_GROUP_INFO);
            printData(resultSet);
        }
    }
}

