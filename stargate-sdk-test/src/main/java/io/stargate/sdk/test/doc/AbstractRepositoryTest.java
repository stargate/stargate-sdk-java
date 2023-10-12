package io.stargate.sdk.test.doc;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.doc.Document;
import io.stargate.sdk.doc.StargateDocumentRepository;
import io.stargate.sdk.doc.domain.PageableQuery;
import io.stargate.sdk.test.doc.domain.Address;
import io.stargate.sdk.test.doc.domain.Person;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class test the document api for namespaces
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
@TestMethodOrder(OrderAnnotation.class)
public abstract class AbstractRepositoryTest implements TestDocClientConstants {
    
    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRepositoryTest.class);
    
    /** Tested Repository. */
    protected static StargateDocumentRepository<Person> personRepository;
    
    // -----------------------------------------
    //        Operation on Repositories
    // -----------------------------------------

    /**
     * Default constructor.
     */
    public AbstractRepositoryTest() {
    }


    /**
     * Test.
     * 
     * @throws InterruptedException
     *  error
     */
    @Test
    @Order(1)
    @DisplayName("01-Create document in a collection")
    public void a_should_create_newDocument() throws InterruptedException {
        LOGGER.info("should_create_newDocument");
        // Given
        Person johnConnor = new Person("John", "Connor", 20, new Address("Syberdyn", 75000));
        // When
        String docId = personRepository.insert(johnConnor);
        // Then
        Assertions.assertNotNull(docId);
        Thread.sleep(500);
        // When
        Optional<Person> john = personRepository.find(docId);
        Assertions.assertTrue(john.isPresent());
        Assertions.assertEquals(20, john.get().getAge());
        
        personRepository.insert("johnConnor", johnConnor);
        
    }
    
    /**
     * Test.
     * 
     * @throws InterruptedException
     *  error
     */
    @Test
    @Order(2)
    @DisplayName("02-Create document with id")
    public void b_should_create_document_with_id() throws InterruptedException {
        LOGGER.info("should_create_newDocument");
        // Given
        Person johnConnor = new Person("John", "Connor", 20, new Address("Syberdyn", 75000));
        // When
        String id = "johnConnor";
        personRepository.insert(id, johnConnor);
        // Then
        Thread.sleep(500);
        // When
        Optional<Person> john = personRepository.find(id);
        Assertions.assertTrue(john.isPresent());
        Assertions.assertEquals(20, john.get().getAge());
    }
    
    /**
     * Test.
     */
    @Test
    @Order(3)
    @DisplayName("03-Count number of items in a collections")
    public void c_should_count_document() {
        // When
        long count = personRepository.count();
        // Then
        Assertions.assertTrue(count > 0);
    }
    
    /**
     * Test.
     */
    @Test
    @Order(4)
    @DisplayName("04-Should find all the documents")
    public void d_should_findall_documents() {
        // When
        List<Person> personList = personRepository
            .findAll()
            .map(Document::getDocument)
            .collect(Collectors.toList());
        // Then
        Assertions.assertNotNull(personList);
        Assertions.assertTrue(personList.size() > 1);
    }
    
    /**
     * Test.
     * 
     * @throws InterruptedException
     *  error
     */
    @Test
    @Order(5)
    @DisplayName("05-Upsert document")
    public void e_should_upsert_document_create() throws InterruptedException {
        LOGGER.info("should_upsert_document_createh");
        // Given
        Person johnConnor = new Person("Johny", "Connor", 20, new Address("Syberdyn", 75000));
        // When
        String id = "johnConnor";
        personRepository.save(id, johnConnor);
        Thread.sleep(500);
        // Then
        Assertions.assertEquals("Johny",personRepository.find(id).get().getFirstname());
    }
    
    /**
     * Test.
     * 
     * @throws InterruptedException
     *  error
     */
    @Test
    @Order(6)
    @DisplayName("06-Delete document")
    public void f_should_delete_document() throws InterruptedException {
        LOGGER.info("should_delete_document");
        // Given
        String saraConnorId = "saraConnor";
        Assertions.assertFalse(personRepository.exists(saraConnorId));
        // When
        Person saraConnor = new Person("Sara", "Connor", 45, new Address("Albuquerk", 75000));
        personRepository.insert(saraConnorId, saraConnor);
        // Then
        Assertions.assertTrue(personRepository.exists(saraConnorId));
        // When ... doom doom
        personRepository.delete(saraConnorId);
        Thread.sleep(500);
        // Then.. judgment day
        Assertions.assertFalse(personRepository.exists(saraConnorId));
        LOGGER.info("Document does not exist");
    }
    
    /**
     * Test.
     */
    @Test
    @Order(8)
    @DisplayName("08-Search Document with a filter Query")
    public void h_should_findPage() {
        // When
        Page<Document<Person>> page1 = personRepository.findPage(PageableQuery.builder().pageSize(1).build());
        // Then
        Assertions.assertNotNull(page1);
        Assertions.assertEquals(1, page1.getResults().size());
        Assertions.assertTrue(page1.getPageState().isPresent());
        Assertions.assertEquals(1, page1.getPageSize());
        // When
        Page<Document<Person>> page2 = personRepository.findPage(PageableQuery.builder()
                .pageSize(1)
                .pageState(page1.getPageState().get())
                .build());
        Assertions.assertEquals(1, page2.getResults().size());
        Assertions.assertNotEquals(page1.getResults().get(0), page2.getResults().get(0));
    }
    
    /**
     * Test.
     */
    @Test
    @Order(9)
    @DisplayName("09-Search Document with a filter Query")
    public void h_should_search() {
        //personRepository.
    }
    

}
