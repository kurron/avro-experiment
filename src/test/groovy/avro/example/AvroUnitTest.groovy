package avro.example

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.avro.AvroFactory
import com.fasterxml.jackson.dataformat.avro.AvroSchema
import groovy.transform.Canonical
import org.apache.avro.Schema
import spock.lang.Specification

import java.util.concurrent.ThreadLocalRandom

/**
 * Exercise the various codec scenarios.
 */
class AvroUnitTest extends Specification {

    @Canonical
    static class User100 {
        String name
    }

    void 'exercise Jackson Avro support'() {

        given: 'an Avro schema and mapper'
        def schemaStream = AvroUnitTest.class.classLoader.getResourceAsStream( 'schemas/user-1.0.0.json' )
        def raw = new Schema.Parser().setValidate( true ).parse( schemaStream )
        def schema = new AvroSchema( raw )
        def mapper = new ObjectMapper( new AvroFactory() )

        and: 'an Avro encoded instance'
        def name = Integer.toHexString( ThreadLocalRandom.current().nextInt( 0, Integer.MAX_VALUE ) )
        def original = new User100( name: name )
        byte[] encoded = mapper.writer( schema ).writeValueAsBytes( original )

        when: 'the instance are recreated'
        User100 decoded = mapper.readerFor( User100.class ).with( schema ).readValue( encoded )

        then: 'encoded and decoded match'
        decoded == original
    }
}
