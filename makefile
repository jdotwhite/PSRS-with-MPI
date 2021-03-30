make: boss.c materials.c boss.h
	mpicc -o boss boss.c -O
clean: 
	rm boss

