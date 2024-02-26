package model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

@Entity
@Table(name= "campo")
public class Campo {
	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	@Column(name = "id", unique = true, nullable = true)
	private int id_campo;

	@OneToMany
    @JoinColumn(name = "campo_id")
	private List<Vid> vids;
	
	@OneToOne
	@JoinColumn(name = "id_bodega")
	private Bodega bodega;
	
	public Campo() {}

	public Campo(Bodega b) {
		this.bodega = b;
		this.vids = new ArrayList<>();
	}

	@Override
	public String toString() {
		return "Campo [id_campo=" + id_campo + ", vids=" + Arrays.toString(vids.toArray()) + ", bodega="
				+ bodega.toString() + "]";
	}

	public void addVid(Vid v) {
		this.vids.add(v);		
	}

	public ArrayList<Vid> getVids() {
		ArrayList<Vid> vids = new ArrayList<>();
		vids.addAll(this.vids);
		return vids;
	}
}
