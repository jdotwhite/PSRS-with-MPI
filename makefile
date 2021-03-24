make: boss.c materials.c boss.h
	mpicc -o boss boss.c

clean: 
	rm boss

