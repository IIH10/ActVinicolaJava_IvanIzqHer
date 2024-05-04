package manager;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Manager {
    private static Manager manager;
    private ArrayList<Document> entradas;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private Document b;
    private Document c;
    private Map<Document, List<Document>> bodegaVidsMap;
    private List<Document> camposRecolectados;

    private Manager() {
        this.entradas = new ArrayList<>();
        this.bodegaVidsMap = new HashMap<>();
        this.camposRecolectados = new ArrayList<>();
    }

    public static Manager getInstance() {
        if (manager == null) {
            manager = new Manager();
        }
        return manager;
    }

    private void createSession() {
        // Establecer la conexión con MongoDB en localhost en el puerto 27017
        mongoClient = MongoClients.create("mongodb://localhost:27017");
        // Seleccionar la base de datos 'dam2tm06uf2p2'
        database = mongoClient.getDatabase("dam2tm06uf2p2");
    }

    public void init() {
        createSession();
        getEntrada();
        manageActions();
        showAllCampos();
        mongoClient.close();
    }

    private void manageActions() {
        for (Document entrada : this.entradas) {
            // Eliminar las comillas simples de la instrucción
            String instruccion = entrada.getString("instruccion").replace("'", "").trim();
            System.out.println(instruccion);
            String[] split = instruccion.toUpperCase().split(" ");
            
            switch (split[0]) {
                case "B":
                    addBodega(split);
                    break;
                case "C":
                    addCampo(split);
                    break;
                case "V":
                    addVid(split);
                    break;
                case "#":
                    vendimia();
                    break;
                default:
                    System.out.println("Instruccion incorrecta");
            }
        }
    }

	private void vendimia() {
	    // Añadir las vides de todas las bodegas a la bodega actual
	    for (Map.Entry<Document, List<Document>> entry : bodegaVidsMap.entrySet()) {
	        Document bodega = entry.getKey();
	        List<Document> vids = entry.getValue();
	        bodega.put("vids", vids);
	    }
	    
	    // Cambiar recolectado a true a todos los campos recolectados
	    for (Document campo : camposRecolectados) {
	        // Mira si el campo tiene vides antes de recolectar
	        if (campo.containsKey("vids")) {
	            campo.put("recolectado", true);
	            // Actualizar el campo en MongoDB
	            MongoCollection<Document> collection = database.getCollection("Campo");
	            Document updateQuery = new Document();
	            updateQuery.append("$set", new Document().append("recolectado", true));
	            collection.updateOne(new Document("_id", campo.get("_id")), updateQuery);
	        }
	    }
	    
	    bodegaVidsMap.clear();  // Limpiar el mapa de bodegas y vides para la próxima vendimia
	}

	private void addVid(String[] split) {
        Document v = new Document();
        v.put("tipo", split[1].toUpperCase());
        v.put("cantidad", Integer.parseInt(split[2]));
        
        // Insertar la vid en MongoDB
        MongoCollection<Document> collection = database.getCollection("Vid");
        collection.insertOne(v);
        
        List<Document> vids = (List<Document>) c.get("vids");
        if (vids == null) {
            vids = new ArrayList<>();
        }
        vids.add(v);
        c.put("vids", vids);
        
        // Actualizar el campo en MongoDB
        Document updateQuery = new Document();
        updateQuery.append("$set", new Document().append("vids", vids));
        collection.updateOne(new Document("_id", c.get("_id")), updateQuery);
        
        // Añadir la vid a la lista de vides de la bodega actual
        List<Document> bodegaVids = bodegaVidsMap.getOrDefault(b, new ArrayList<>());
        bodegaVids.add(v);
        bodegaVidsMap.put(b, bodegaVids);
    }

	private void addCampo(String[] split) {
        c = new Document();
        c.put("bodega", b.get("_id"));
        
        // Insertar el campo en MongoDB
        MongoCollection<Document> collection = database.getCollection("Campo");
        collection.insertOne(c);
        
        // Añadir campo a la lista de campos recolectados
        camposRecolectados.add(c);
    }

	private void addBodega(String[] split) {
		b = new Document();
		b.put("nombre", split[1]);
		
		// Insertar la bodega en MongoDB
		MongoCollection<Document> collection = database.getCollection("Bodega");
		collection.insertOne(b);
	}

	private void getEntrada() {
        MongoCollection<Document> collection = database.getCollection("Entrada");
        for (Document doc : collection.find()) {
            this.entradas.add(doc);
        }
    }

	private void showAllCampos() {
		MongoCollection<Document> collection = database.getCollection("Campo");
		for (Document doc : collection.find()) {
			System.out.println(doc.toJson());
		}
	}
}
