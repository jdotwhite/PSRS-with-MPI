make: boss.c materials.c boss.h
	mpicc -o boss boss.c
	scp /Users/joshuawhite/PSRS-with-MPI/boss/ ubuntu@10.2.7.198:
	scp /Users/joshuawhite/PSRS-with-MPI/boss/ ubuntu@10.2.9.205:

clean: 
	rm boss

