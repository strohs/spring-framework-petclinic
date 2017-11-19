package org.springframework.samples.petclinic.repository.jdbc;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.samples.petclinic.config.BusinessConfig;
import org.springframework.samples.petclinic.model.Owner;
import org.springframework.samples.petclinic.model.Person;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test classes for learning jdbcTemplate functionality
 * User: Cliff
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles({"jdbc"})
@ContextConfiguration( classes = BusinessConfig.class )
@Sql( scripts = {"/db/hsqldb/initDB.sql","/db/hsqldb/populateDB.sql"} )
public class JdbcTest {

    private static final Integer TYPE_COUNT = 6;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void queryForObjectTest() throws Exception {
        String sql =  "SELECT COUNT(*) FROM types";
        Integer count = jdbcTemplate.queryForObject( sql, Integer.class );

        assertThat( count, is( TYPE_COUNT ) );
    }

    @Test
    public void shouldSelectOwnerFirstName() throws Exception {
        String sql = "SELECT o.first_name FROM owners o WHERE o.id = ?";
        Integer id = 1;
        String firstName = "George";
        String selectedFirstName = jdbcTemplate.queryForObject( sql, String.class, id );

        assertThat( selectedFirstName, is( firstName ) );
    }

    @Test
    public void queryForMapTest() throws Exception {
        String sql = "SELECT * FROM pets WHERE id = ?";
        Integer petId = 1;

        Map<String,Object> petMap = jdbcTemplate.queryForMap( sql, petId );
        assertThat( petMap, notNullValue() );
        assertThat( petMap.get( "NAME" ).toString(), is( "Leo") );
        printMap( petMap );
    }

    @Test
    public void queryForListTest() throws Exception {
        String sql = "SELECT * FROM pets WHERE owner_id = ?";
        Integer ownerId = 3;

        List<Map<String,Object>> pets = jdbcTemplate.queryForList( sql, ownerId );
        assertThat( pets, notNullValue() );

        printListOfMap( pets );
    }

    @Test
    public void rowMapperToOwnerTest() throws Exception {
        String sql = "SELECT * FROM owners WHERE id = ?";
        Integer ownerId = 9;

        Owner owner = jdbcTemplate.queryForObject( sql, ownerRowMapper, ownerId );

        assertThat( owner, notNullValue() );
        System.out.println( owner.toString() );
    }

    @Test
    public void rowMapperToListOfOwnerTest() throws Exception {
        String sql = "SELECT * FROM owners WHERE id >= ?";
        Integer ownerId = 9;
        int ownerCount = 2;

        List<Owner> owners = jdbcTemplate.query( sql, ownerRowMapper, ownerId );

        assertThat( owners, notNullValue() );
        assertThat( owners.size(), is(ownerCount) );
        owners.forEach( System.out::println );
    }

    @Test
    public void resultSetExtractorTest() throws Exception {
        String sql = "SELECT v.first_name, v.last_name, s.name " +
            "FROM vets v, specialties s, vet_specialties vs " +
            "WHERE v.id = ? AND v.id = vs.vet_id AND vs.specialty_id = s.id";
        Integer vetId = 3;

        VetSpecs vetSpecs = jdbcTemplate.query( sql, vetSpecsResultSetExtractor, vetId );

        assertThat( vetSpecs, notNullValue() );
        assertThat( vetSpecs.specialties.size(), is(2) );
        System.out.println( vetSpecs.toString() );
    }

    @Test
    public void rowCallBackHandlerTest() throws Exception {
        String sql = "SELECT * FROM owners";
        int ownerCount = 10;
        RowCountCallbackHandler rowCounter = new RowCountCallbackHandler();

        jdbcTemplate.query( sql, rowCounter );

        assertThat( rowCounter.rowCount, is( ownerCount ) );
    }

    @Test
    @Transactional
    public void updatePetTypeTest() throws Exception {
        String sql = "UPDATE types SET name = ? WHERE id = ?";
        String selectNameSql = "SELECT name FROM types WHERE id = ?";
        String newName = "elephant";
        Integer id = 6;

        int updatedRows = jdbcTemplate.update( sql, newName, id );

        assertThat( updatedRows, is(1) );
        String name = jdbcTemplate.queryForObject( selectNameSql, String.class ,id );
        assertThat( name, is( name ) );

    }

    @Test
    @Transactional
    public void insertPetTypeTest() throws Exception {
        String sql = "INSERT INTO types (id, name) VALUES (?,?)";
        String selectSql = "SELECT id,name FROM types WHERE id = ?";
        String newName = "elephant";
        Integer newId = 7;

        int updatedRows = jdbcTemplate.update( sql, newId, newName );

        assertThat( updatedRows, is(1) );
        Map<String,Object> map = jdbcTemplate.queryForMap( selectSql, newId );
        assertThat( map, notNullValue() );
        assertThat( map.get( "id" ), is( newId ) );
        assertThat( map.get( "name" ), is( newName ) );

    }

    @Test
    @Transactional
    public void insertPetTypeWithGeneratedKeyTest() throws Exception {
        String sql = "INSERT INTO types (name) VALUES (?)";
        String selectSql = "SELECT id,name FROM types WHERE id = ?";
        String newName = "elephant";
        Integer newId = 7;

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(
            connection -> {
                PreparedStatement ps = connection.prepareStatement( sql, new String[] {"id"});
                ps.setString(1, newName);
                return ps;
            },
            keyHolder);

        assertThat( keyHolder.getKey(), is(newId) );
        Map<String,Object> map = jdbcTemplate.queryForMap( selectSql, newId );
        assertThat( map, notNullValue() );
        assertThat( map.get( "id" ), is( newId ) );
        assertThat( map.get( "name" ), is( newName ) );

    }

    private RowMapper<Owner> ownerRowMapper = ( ResultSet rs, int rowNum ) -> {
        Owner owner = new Owner();
        owner.setId( rs.getInt( "id" ) );
        owner.setFirstName( rs.getString( "first_name" ) );
        owner.setLastName( rs.getString( "last_name" ) );
        owner.setAddress( rs.getString( "address" ) );
        owner.setCity( rs.getString( "city" ) );
        owner.setTelephone( rs.getString( "telephone" ) );
        return owner;
    };

    private ResultSetExtractor<VetSpecs> vetSpecsResultSetExtractor = rs -> {
        VetSpecs vetSpecs = new VetSpecs();
        while ( rs.next() ) {
            vetSpecs.setFirstName( rs.getString( "first_name" ) );
            vetSpecs.setLastName( rs.getString( "last_name" ) );
            vetSpecs.addSpecialty( rs.getString( "name" ) );
        }
         return vetSpecs;
    };

    private void printMap( Map<String,Object> map ) {
        map.forEach( ( s, o ) -> System.out.println( "key:" + s + " value:" + o ) );
    }

    private void printListOfMap( List<Map<String,Object>> maps ) {
        maps.forEach( stringObjectMap ->  {
            printMap( stringObjectMap );
            System.out.println("-------------------------");
        } );
    }

    private class VetSpecs {
        private String firstName;
        private String lastName;
        private List<String> specialties = new ArrayList<>();

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName( String firstName ) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName( String lastName ) {
            this.lastName = lastName;
        }

        public List<String> getSpecialties() {
            return specialties;
        }

        public void setSpecialties( List<String> specialties ) {
            this.specialties = specialties;
        }

        public void addSpecialty( String specialty ) {
            this.specialties.add( specialty );
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder( "VetSpecs{" );
            sb.append( "firstName='" ).append( firstName ).append( '\'' );
            sb.append( ", lastName='" ).append( lastName ).append( '\'' );
            sb.append( ", specialties=" ).append( specialties );
            sb.append( '}' );
            return sb.toString();
        }
    }

    private class RowCountCallbackHandler implements RowCallbackHandler {
        private int rowCount;

        @Override
        public void processRow( ResultSet rs ) throws SQLException {
            rowCount++;
        }

        public int getRowCount() {
            return rowCount;
        }
    }

}
