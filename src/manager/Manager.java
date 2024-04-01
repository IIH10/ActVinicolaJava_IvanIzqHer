package manager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;

import model.Bodega;
import model.Campo;
import model.Entrada;
import model.Vid;
import utils.TipoVid;

public class Manager {
	private static Manager manager;
	private ArrayList<Entrada> entradas;
	private Session session;
	private Transaction tx;
	private Bodega b;
	private Campo c;
	private Map<Bodega, List<Vid>> bodegaVidsMap;
	private List<Campo> camposRecolectados;

	private Manager () {
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
		org.hibernate.SessionFactory sessionFactory = new Configuration().configure().buildSessionFactory();
    	session = sessionFactory.openSession();
	}

	public void init() {
		createSession();
		getEntrada();
		manageActions();
		showAllCampos();
		showCantidadVidByCampo();
		session.close();
	}

	private void manageActions() {
		for (Entrada entrada : this.entradas) {
			try {
				System.out.println(entrada.getInstruccion());
				switch (entrada.getInstruccion().toUpperCase().split(" ")[0]) {
					case "B":
						addBodega(entrada.getInstruccion().split(" "));
						break;
					case "C":
						addCampo(entrada.getInstruccion().split(" "));
						break;
					case "V":
						addVid(entrada.getInstruccion().split(" "));
						break;
					case "#":
						vendimia();
						break;
					default:
						System.out.println("Instruccion incorrecta");
				}
			} catch (HibernateException e) {
				e.printStackTrace();
				if (tx != null) {
					tx.rollback();
				}
			}
		}
	}

	private void vendimia() {
	    // Añadir las vides de todas las bodegas a la bodega actual
	    for (Map.Entry<Bodega, List<Vid>> entry : bodegaVidsMap.entrySet()) {
	        Bodega bodega = entry.getKey();
	        List<Vid> vids = entry.getValue();
	        bodega.getVids().addAll(vids);
	    }
	    
	    tx = session.beginTransaction();
	    session.save(b);
	    
	    // Cambiar recolectado a true a todos los campos recolectados
	    for (Campo campo : camposRecolectados) {
	        // Mira si el campo tiene vides antes de recolectar
	        if (!campo.getVids().isEmpty()) {
	            campo.setRecolectado(true);
	            session.save(campo);
	        }
	    }
	    
	    tx.commit();
	    
	    bodegaVidsMap.clear();  // Limpiar el mapa de bodegas y vides para la próxima vendimia
	}



	private void addVid(String[] split) {
        Vid v = new Vid(TipoVid.valueOf(split[1].toUpperCase()), Integer.parseInt(split[2]));
        tx = session.beginTransaction();
        session.save(v);
        
        c.addVid(v);
        session.save(c);
        
        tx.commit();
        
        // Añadir la vid a la lista de vides de la bodega actual
        List<Vid> vids = bodegaVidsMap.getOrDefault(b, new ArrayList<>());
        vids.add(v);
        bodegaVidsMap.put(b, vids);
    }

	private void addCampo(String[] split) {
        c = new Campo(b);
        tx = session.beginTransaction();
        
        int id = (Integer) session.save(c);
        c = session.get(Campo.class, id);
        
        tx.commit();

        // Añadir campo a la lista de campos recolectados
        camposRecolectados.add(c);
    }

	private void addBodega(String[] split) {
		b = new Bodega(split[1]);
		tx = session.beginTransaction();
		
		int id = (Integer) session.save(b);
		b = session.get(Bodega.class, id);
		
		tx.commit();
		
	}

	private void getEntrada() {
		tx = session.beginTransaction();
		Query q = session.createQuery("select e from Entrada e");
		this.entradas.addAll(q.list());
		tx.commit();
	}

	private void showAllCampos() {
		tx = session.beginTransaction();
		Query q = session.createQuery("select c from Campo c");
		List<Campo> list = q.list();
		for (Campo c : list) {
			System.out.println(c);
		}
		tx.commit();
	}
	
	public void showCantidadVidByCampo() {
	    tx = session.beginTransaction();
	    Query q = session.createQuery("select c.id, sum(v.cantidad) from Campo c join c.vids v group by c.id");
	    List<Object[]> list = q.list();
	    for (Object[] row : list) {
	        Integer idCampo = (Integer) row[0];
	        Long cantidad = (Long) row[1];
	        System.out.println("Id Campo: " + idCampo + ", Total Vid Cantidad: " + cantidad);
	    }
	    tx.commit();
	}

}