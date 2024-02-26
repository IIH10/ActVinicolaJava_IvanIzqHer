package model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import utils.TipoVid;

@Entity
@Table(name= "vid")
public class Vid {
	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	@Column(name = "id", unique = true, nullable = true)
	private int id;
	@Column(name = "tipo_vid", nullable = true)
	private TipoVid vid;
	@Column(name = "cantidad", nullable = true)
	private int cantidad;
	
	public Vid() {}
		
	public Vid(TipoVid vid, int cantidad) {
		this.vid = vid;
		this.cantidad = cantidad;
	}
	public int getId() {
		return this.id;
	}
	public TipoVid getVid() {
		return vid;
	}
	public int getCantidad() {
		return cantidad;
	}
	@Override
	public String toString() {
		return "Vid [vid=" + (vid.equals("0") ? "blanca" : "negra")  + ", cantidad=" + cantidad + "]";
	}
}
