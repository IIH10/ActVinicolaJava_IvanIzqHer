package model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name= "Entrada")
public class Entrada {
	@Id
	@Column(name = "id")
	private String valor;

	@Column(name = "instruccion")
	private String instruccion;
	
	public String getInstruccion() {
		return this.instruccion;
	}
}
