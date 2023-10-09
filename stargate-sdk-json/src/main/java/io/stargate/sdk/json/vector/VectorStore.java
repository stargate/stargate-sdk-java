package io.stargate.sdk.json.vector;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.json.JsonCollectionClient;
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.JsonResult;
import io.stargate.sdk.json.domain.SelectQuery;
import io.stargate.sdk.json.domain.odm.Record;
import lombok.NonNull;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Crud Repository for Vector entities
 *
 * @param <T>
 *     curren vector object
 */
public class VectorStore<T> {

    private final JsonCollectionClient collectionClient;

    /** Keep ref to the generic. */
    private final Class<T> docClass;

    /**
     * Default constructor.
     *
     * @param col
     *      collection client parent
     * @param clazz
     *      working bean class
     */
    public VectorStore(JsonCollectionClient col, Class<T> clazz) {
        this.collectionClient = col;
        this.docClass  = clazz;
    }

    /**
     * Find by Id.
     *
     * @param id
     *      identifier
     * @return
     *      object if presents
     */
    public Optional<Record<T>> findById(@NonNull String id) {
        return collectionClient.findById(id, docClass);
    }

    /**
     * Find by Id.
     *
     * @param id
     *      identifier
     * @return
     *      object if presents
     */
    public Optional<Record<T>> findByVector(@NonNull List<Float> embeddings) {
        return collectionClient.findOneByVector(embeddings, docClass);
    }

    //------- InsertOne -----

    /**
     * Create a new document a generating identifier.
     *
     * @param current
     *      object Mapping
     * @return
     *      an unique identifier for the document
     */
    public String insert(@NonNull Record<T> current) {
        return collectionClient.insertOne(current.asJsonRecord());
    }

    public String insert(String id, @NonNull T current, List<Float> vector) {
        return collectionClient.insertOne(id, current, vector);
    }

    public String insert(@NonNull T current, List<Float> vector) {
        return collectionClient.insertOne(current, vector);
    }

    //------- Count -----

    /**
     * Count items in the collection.
     *
     * @return
     *      number of records.
     */
    public int count() {
        return collectionClient.countDocuments();
    }

    /**
     * Count items with a matching request.
     *
     * @param filter
     *      provided request
     * @return
     *      number of items matching the request
     */
    public int count(Filter filter) {
        if (filter == null) return count();
        return collectionClient.countDocuments(filter);
    }

    //------- Find -----

    public Page<Record<T>> findP(SelectQuery query) {
        Page<JsonResult> page = collectionClient.findAllPageable(query);
        Page<Record<T>> vectorPage = new Page<Record<T>>(page.getPageSize(), page.getPageState().orElse(null));
        vectorPage.setResults(page.getResults().stream().map(r -> new Record<>(r, docClass)).collect(Collectors.toList()));
        return vectorPage;
    }

    /**
     * Delete a document from its iid.
     *
     * @param docId
     *          document identifier
     *
    public void deleteById(String docId) {
        collectionClient.document(docId).delete();
    }

    /**
     * Check existence of a document from its id.
     *
     * @param docId
     *      document identifier
     * @return
     *      existence status
     *
    public boolean exists(String docId) {
        return collectionClient.document(docId).exist();
    }

    /**
     * Create a new document a generating identifier.
     *
     * @param p
     *      working document
     * @return
     *      an unique identifier for the document
     *
    public String insert(DOC p) {
        return collectionClient.create(p);
    }

    /**
     * Create if not exist with defined ID.
     *
     * @param docId
     *      expected document id
     * @param doc
     *      document
     *
    public void insert(String docId, DOC doc) {
        collectionClient.document(docId).upsert(doc);
    }

    /**
     * Create if not exist with defined ID.
     *
     * @param docId
     *      expected document id
     * @param doc
     *      document
     *
    public void save(String docId, DOC doc) {
        collectionClient.document(docId).upsert(doc);
    }

    /**
     * Evaluation on which collection we are working.
     *
     * @return
     *      collection identifier
     *
    public String getCollectionName() {
        return collectionClient.getCollectionName();
    }

    /**
     * Find a person from ids unique identifier.
     *
     * @param docId
     *      document Id
     * @return
     *      the object only if present
     *
    public Optional<DOC> find(String docId) {
        return collectionClient.document(docId).find(docClass);
    }

    /**
     * Retrieve all documents from the collection.
     *
     * @return
     *      every document of the collection
     *
    public Stream<Document<DOC>> findAll() {
        return collectionClient.findAll(docClass);
    }

    /**
     * Search document based on a search query
     *
     * @param query
     *      current query
     * @return
     *      all the element matching
     *
    public Stream<Document<DOC>> findAll(Query query) {
        return collectionClient.findAll(query, docClass);
    }

    /**
     * Search document with attributes.
     *
     * @param query
     *      current query
     * @return
     *      result page
     *
    public Page<Document<DOC>> findPage(PageableQuery query) {
        return collectionClient.findPage(query, docClass);
    }

    /**
     * Read Collection Name.
     *
     * @param myClass
     *      my current class
     * @return
     *      name of the collection
     *
    private String getCollectionName(Class<T> myClass) {
        Vector ann = myClass.getAnnotation(Collection.class);
        if (null != ann && ann.value() !=null && !ann.value().equals("")) {
            return ann.value();
        } else {
            return myClass.getSimpleName().toLowerCase();
        }
    }*/

}



